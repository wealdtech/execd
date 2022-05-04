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

	"github.com/attestantio/go-execution-client/types"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

func (s *Service) catchup(ctx context.Context, md *metadata) {
	chainHeight, err := s.chainHeightProvider.ChainHeight(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain chain height")
		return
	}

	if chainHeight < s.trackDistance {
		log.Trace().Msg("Chain height not above track distance; not processing")
		return
	}
	maxHeight := chainHeight - s.trackDistance
	log.Trace().Uint32("chain_height", chainHeight).Uint32("max_height", maxHeight).Msg("Fetch parameters")

	mdMu := &sync.Mutex{}
	var wg sync.WaitGroup
	// Fetch for each address in parallel.
	s.addressesMu.RLock()
	for _, address := range s.addresses {
		latestHeight, exists := md.LatestHeights[fmt.Sprintf("%#x", address)]
		if !exists {
			latestHeight = -1
		}

		wg.Add(1)
		go func(ctx context.Context,
			md *metadata,
			mdMu *sync.Mutex,
			address types.Address,
			startHeight uint32,
			endHeight uint32,
		) {
			defer wg.Done()
			s.catchupAddress(ctx, md, mdMu, address, startHeight, endHeight)
		}(ctx, md, mdMu, address, uint32(latestHeight+1), maxHeight)
	}
	s.addressesMu.RUnlock()
	wg.Wait()
}

func (s *Service) catchupAddress(ctx context.Context,
	md *metadata,
	mdMu *sync.Mutex,
	address types.Address,
	startHeight uint32,
	endHeight uint32,
) {
	log := log.With().Str("address", fmt.Sprintf("%#x", address)).Logger()

	batchSize := uint32(1024)
	balances := make([]*execdb.Balance, 0)
	for height := startHeight; height <= endHeight; height += batchSize {
		batchEnd := height + batchSize
		if batchEnd > endHeight {
			batchEnd = endHeight
		}

		for balanceHeight := height; balanceHeight <= batchEnd; balanceHeight++ {
			log := log.With().Uint32("height", balanceHeight).Logger()

			block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", balanceHeight))
			if err != nil {
				log.Error().Err(err).Msg("Failed to obtain block")
				return
			}

			balance, err := s.balancesProvider.Balance(ctx, address, fmt.Sprintf("%d", balanceHeight))
			if err != nil {
				log.Error().Err(err).Msg("Failed to obtain balance")
				return
			}
			log.Trace().Str("balance", balance.String()).Msg("Obtained balance")
			s.currentBalancesMu.Lock()
			oldBalance := s.currentBalances[address]
			s.currentBalancesMu.Unlock()
			newBalance := decimal.NewFromBigInt(balance, 0)
			if !oldBalance.Equal(newBalance) {
				log.Trace().Str("old_balance", oldBalance.String()).Str("new_balance", newBalance.String()).Msg("Updated balance")
				balances = append(balances, &execdb.Balance{
					Address:  address,
					Currency: "WEI",
					From:     block.Timestamp(),
					Amount:   newBalance,
				})
				// Update cache.
				s.currentBalancesMu.Lock()
				s.currentBalances[address] = newBalance
				s.currentBalancesMu.Unlock()
			}
		}

		ctx, cancel, err := s.balancesSetter.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction")
			return
		}
		if len(balances) > 0 {
			if err := s.balancesSetter.SetBalances(ctx, balances); err != nil {
				log.Error().Err(err).Msg("Failed to store balances")
				cancel()
				return
			}
		}

		mdMu.Lock()
		md.LatestHeights[fmt.Sprintf("%#x", address)] = int64(batchEnd)
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to set metadata")
			return
		}
		mdMu.Unlock()

		if err := s.balancesSetter.CommitTx(ctx); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to commit transaction")
			return
		}

		monitorBlocksProcessed(int(batchEnd+1-startHeight), height)
	}
}
