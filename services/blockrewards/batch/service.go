// Copyright © 2021 Weald Technology Trading.
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
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/scheduler"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	scheduler                     scheduler.Service
	blocksProvider                execdb.BlocksProvider
	transactionsProvider          execdb.TransactionsProvider
	transactionStateDiffsProvider execdb.TransactionStateDiffsProvider
	blockRewardsSetter            execdb.BlockRewardsSetter
	processConcurrency            int64
	interval                      time.Duration
	activitySem                   *semaphore.Weighted
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
	log = zerologger.With().Str("service", "blockrewards").Str("impl", "batch").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		scheduler:                     parameters.scheduler,
		blocksProvider:                parameters.blocksProvider,
		transactionsProvider:          parameters.transactionsProvider,
		transactionStateDiffsProvider: parameters.transactionStateDiffsProvider,
		blockRewardsSetter:            parameters.blockRewardsSetter,
		processConcurrency:            parameters.processConcurrency,
		interval:                      parameters.interval,
		activitySem:                   semaphore.NewWeighted(1),
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

	log.Info().Int64("height", md.LatestHeight).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Info().Int64("height", md.LatestHeight).Msg("Caught up; starting periodic update")

	runtimeFunc := func(_ context.Context, _ any) (time.Time, error) {
		return time.Now().Add(s.interval), nil
	}

	if err := s.scheduler.SchedulePeriodicJob(ctx,
		"update",
		"Update block rewards",
		runtimeFunc,
		nil,
		s.updateOnScheduleTick,
		nil,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to schedule block rewards updates.")
	}
}

func (s *Service) updateOnScheduleTick(ctx context.Context, _ any) {
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
