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

package individual

import (
	"context"
	"time"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/scheduler"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	scheduler                   scheduler.Service
	chainHeightProvider         execclient.ChainHeightProvider
	blocksProvider              execclient.BlocksProvider
	blockReplaysProvider        execclient.BlockReplaysProvider
	issuanceProvider            execclient.IssuanceProvider
	transactionReceiptsProvider execclient.TransactionReceiptsProvider
	blocksSetter                execdb.BlocksSetter
	transactionsSetter          execdb.TransactionsSetter
	transactionStateDiffsSetter execdb.TransactionStateDiffsSetter
	eventsSetter                execdb.EventsSetter
	trackDistance               uint32
	activitySem                 *semaphore.Weighted
	enableTransactions          bool
	enableTransactionEvents     bool
	enableBalanceChanges        bool
	enableStorageChanges        bool
	interval                    time.Duration
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
	log = zerologger.With().Str("service", "blocks").Str("impl", "individual").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		scheduler:                   parameters.scheduler,
		chainHeightProvider:         parameters.chainHeightProvider,
		blocksProvider:              parameters.blocksProvider,
		blockReplaysProvider:        parameters.blockReplaysProvider,
		issuanceProvider:            parameters.issuanceProvider,
		transactionReceiptsProvider: parameters.transactionReceiptsProvider,
		blocksSetter:                parameters.blocksSetter,
		transactionsSetter:          parameters.transactionsSetter,
		transactionStateDiffsSetter: parameters.transactionStateDiffsSetter,
		eventsSetter:                parameters.eventsSetter,
		trackDistance:               parameters.trackDistance,
		activitySem:                 semaphore.NewWeighted(1),
		enableTransactions:          parameters.enableTransactions,
		enableTransactionEvents:     parameters.enableTransactionEvents,
		enableBalanceChanges:        parameters.enableBalanceChanges,
		enableStorageChanges:        parameters.enableStorageChanges,
		interval:                    parameters.interval,
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
	log.Info().Msg("Caught up; starting periodic update")

	runtimeFunc := func(ctx context.Context, data interface{}) (time.Time, error) {
		return time.Now().Add(s.interval), nil
	}

	if err := s.scheduler.SchedulePeriodicJob(ctx,
		"Updates",
		"Block updates",
		runtimeFunc,
		nil,
		s.updateOnScheduleTick,
		nil,
	); err != nil {
		log.Fatal().Err(err).Msg("Failed to schedule block updates.")
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
	log.Trace().Msg("Caught up")
}
