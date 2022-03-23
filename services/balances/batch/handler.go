// Copyright © 2022 Weald Technology Trading.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/attestantio/go-execution-client/types"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/util"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"
)

type batchData struct {
	balances map[types.Address]decimal.Decimal
}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	chainHeight, err := s.chainHeightProvider.ChainHeight(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain chain height")
		return
	}
	safetyMargin := uint32(64) // Run 64 blocks behind for safety; could make this much tighter with integration With CL.
	maxHeight := chainHeight - safetyMargin
	log.Trace().Uint32("chain_height", chainHeight).Uint32("fetch_height", maxHeight).Msg("Fetch parameters")

	// We processing of a subsequent batch to occur whilst storage of batch is taking place.  Use a semaphore
	// to ensure that only 1 write is queued at a time.
	writeSem := semaphore.NewWeighted(1)

	// If the store operation fails we need to know about it.
	var storeFailed atomic.Bool
	storeFailed.Store(false)

	// Batch process the updates, fetching balances in parallel
	for height := uint32(md.LatestHeight + 1); height <= maxHeight; height++ {
		if storeFailed.Load() {
			return
		}

		bd := &batchData{
			balances: make(map[types.Address]decimal.Decimal, len(s.addresses)),
		}

		block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", height))
		if err != nil {
			log.Error().Err(err).Msg("Failed to obtain block")
		}

		if _, err = util.Scatter(len(s.addresses), int(s.processConcurrency), func(offset int, entries int, mu *sync.RWMutex) (interface{}, error) {
			for i := offset; i < offset+entries; i++ {
				balance, err := s.balancesProvider.Balance(ctx, s.addresses[i], fmt.Sprintf("%d", height))
				if err != nil {
					return nil, err
				}
				mu.Lock()
				bd.balances[s.addresses[i]] = decimal.NewFromBigInt(balance, 0)
				mu.Unlock()
				log.Trace().Str("address", fmt.Sprintf("%#x", s.addresses[i])).Uint32("height", height).Str("balance", balance.String()).Msg("Fetched balance")
			}
			return nil, nil
		}); err != nil {
			log.Error().Err(err).Msg("Failed to batch fetch balances")
			return
		}

		if err := writeSem.Acquire(ctx, 1); err != nil {
			log.Error().Err(err).Msg("Failed to acquire write semaphore")
		}
		go func(ctx context.Context,
			md *metadata,
			bd *batchData,
		) {
			if storeFailed.Load() {
				return
			}
			if err := s.store(ctx, md, bd, block.Number(), block.Timestamp()); err != nil {
				log.Error().Err(err).Msg("Failed to store")
				storeFailed.Store(true)
				writeSem.Release(1)
				return
			}
			log.Trace().Msg("Stored")
			writeSem.Release(1)
		}(ctx, md, bd)
	}
}

func (s *Service) store(ctx context.Context,
	md *metadata,
	bd *batchData,
	height uint32,
	timestamp time.Time,
) error {
	ctx, cancel, err := s.balancesSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	// Build a list of updated balances.
	s.currentBalancesMu.Lock()
	balances := make([]*execdb.Balance, 0, len(bd.balances))
	for address, balance := range bd.balances {
		if !s.currentBalances[address].Equal(balance) {
			log.Trace().Str("address", fmt.Sprintf("%#x", address)).Str("old_balance", s.currentBalances[address].String()).Str("new_balance", balance.String()).Msg("Balance has changed")
			balances = append(balances, &execdb.Balance{
				Address:  address,
				Currency: "WEI",
				From:     timestamp,
				Amount:   balance,
			})
		}
	}
	s.currentBalancesMu.Unlock()

	if len(balances) > 0 {
		if err := s.balancesSetter.SetBalances(ctx, balances); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set balances")
		}
	}

	md.LatestHeight = int64(height)
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.balancesSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	// Update cache.
	s.currentBalancesMu.Lock()
	for _, balance := range balances {
		s.currentBalances[balance.Address] = balance.Amount
	}
	s.currentBalancesMu.Unlock()

	monitorBlockProcessed(height)

	return nil
}
