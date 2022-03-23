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

	execclient "github.com/attestantio/go-execution-client"
	"github.com/attestantio/go-execution-client/types"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/scheduler"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	scheduler           scheduler.Service
	chainHeightProvider execclient.ChainHeightProvider
	balancesProvider    execclient.BalancesProvider
	blocksProvider      execclient.BlocksProvider
	balancesSetter      execdb.BalancesSetter
	dbBalancesProvider  execdb.BalancesProvider
	addresses           []types.Address
	processConcurrency  int64
	interval            time.Duration
	activitySem         *semaphore.Weighted
	currentBalances     map[types.Address]decimal.Decimal
	currentBalancesMu   sync.Mutex
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "balances").Str("impl", "batch").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		scheduler:           parameters.scheduler,
		chainHeightProvider: parameters.chainHeightProvider,
		balancesProvider:    parameters.balancesProvider,
		blocksProvider:      parameters.blocksProvider,
		balancesSetter:      parameters.balancesSetter,
		dbBalancesProvider:  parameters.dbBalancesProvider,
		addresses:           parameters.addresses,
		processConcurrency:  parameters.processConcurrency,
		interval:            parameters.interval,
		activitySem:         semaphore.NewWeighted(1),
		currentBalances:     make(map[types.Address]decimal.Decimal, len(parameters.addresses)),
	}

	// Update to current block before starting (in the background).
	go s.updateOnRestart(ctx, parameters.startHeight)

	return s, nil
}

func (s *Service) updateOnRestart(ctx context.Context, startHeight int64) {
	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startHeight >= 0 {
		// Explicit requirement to start at a given height.
		// Subtract one to state that the block higher than the required start is the last processed.
		md.LatestHeight = startHeight - 1
	}

	// Populate the balance cache.
	if err := s.populateBalanceCache(ctx, md.LatestHeight); err != nil {
		log.Fatal().Err(err).Msg("Failed to populate balance cache")
	}
	log.Info().Int64("height", md.LatestHeight).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Info().Int64("height", md.LatestHeight).Msg("Caught up; starting periodic update")

	runtimeFunc := func(ctx context.Context, data interface{}) (time.Time, error) {
		return time.Now().Add(s.interval), nil
	}

	if err := s.scheduler.SchedulePeriodicJob(ctx,
		"Balances",
		"Balance updates",
		runtimeFunc,
		nil,
		s.updateOnScheduleTick,
		nil,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to schedule balance updates.")
	}
}

func (s *Service) updateOnScheduleTick(ctx context.Context, data interface{}) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata for update")
		return
	}

	log.Trace().Int64("height", md.LatestHeight).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Trace().Int64("height", md.LatestHeight).Msg("Caught up")
}

func (s *Service) populateBalanceCache(ctx context.Context, height int64) error {
	// Start by setting all balances to 0.
	s.currentBalancesMu.Lock()
	for _, address := range s.addresses {
		s.currentBalances[address] = decimal.NewFromInt(0)
	}
	s.currentBalancesMu.Unlock()

	// If we are starting early then we don't need to do anything else.
	if height < 0 {
		return nil
	}

	// Fetch the block at the given height for its timestamp.
	block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", height))
	if err != nil {
		return errors.Wrap(err, "failed to obtain block")
	}

	// Obtain the latest balance for each address.
	holders := make([][]byte, 1)
	timestamp := block.Timestamp()
	for _, address := range s.addresses {
		holders[0] = address[:]
		balances, err := s.dbBalancesProvider.Balances(ctx, &execdb.BalanceFilter{
			To:       &timestamp,
			Order:    execdb.OrderLatest,
			Limit:    1,
			Currency: "WEI",
			Holders:  holders,
		})
		if err != nil {
			return errors.Wrap(err, "failed to obtain database balances")
		}

		s.currentBalancesMu.Lock()
		for _, balance := range balances {
			//			address := types.Address{}
			//			copy(address[:], balance.Address)
			//			s.currentBalances[address] = balance.Amount
			s.currentBalances[balance.Address] = balance.Amount
			log.Trace().Str("address", fmt.Sprintf("%#x", address)).Str("balance", balance.Amount.String()).Msg("Initial balance")
		}
		s.currentBalancesMu.Unlock()
	}

	return nil
}
