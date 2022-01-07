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
	"errors"
	"time"

	"github.com/rs/zerolog"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/metrics"
	"github.com/wealdtech/execd/services/scheduler"
)

type parameters struct {
	logLevel                      zerolog.Level
	monitor                       metrics.Service
	scheduler                     scheduler.Service
	blocksProvider                execdb.BlocksProvider
	transactionsProvider          execdb.TransactionsProvider
	transactionStateDiffsProvider execdb.TransactionStateDiffsProvider
	blockMEVsSetter               execdb.BlockMEVsSetter
	startHeight                   int64
	processConcurrency            int64
	interval                      time.Duration
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(*parameters)
}

type parameterFunc func(*parameters)

func (f parameterFunc) apply(p *parameters) {
	f(p)
}

// WithLogLevel sets the log level for the module.
func WithLogLevel(logLevel zerolog.Level) Parameter {
	return parameterFunc(func(p *parameters) {
		p.logLevel = logLevel
	})
}

// WithMonitor sets the monitor for the module.
func WithMonitor(monitor metrics.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.monitor = monitor
	})
}

// WithScheduler sets the scheduler for the module.
func WithScheduler(scheduler scheduler.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.scheduler = scheduler
	})
}

// WithBlocksProvider sets the blocks provider for this module.
func WithBlocksProvider(provider execdb.BlocksProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blocksProvider = provider
	})
}

// WithTransactionsProvider sets the transactions provider for this module.
func WithTransactionsProvider(provider execdb.TransactionsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionsProvider = provider
	})
}

// WithTransactionStateDiffsProvider sets the transaction sate diffs provider for this module.
func WithTransactionStateDiffsProvider(provider execdb.TransactionStateDiffsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionStateDiffsProvider = provider
	})
}

// WithBlockMEVsSetter sets the block MEV setter for this module.
func WithBlockMEVsSetter(setter execdb.BlockMEVsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blockMEVsSetter = setter
	})
}

// WithStartHeight sets the start height for this module.
func WithStartHeight(startHeight int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startHeight = startHeight
	})
}

// WithProcessConcurrency sets the concurrency for the service.
func WithProcessConcurrency(concurrency int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.processConcurrency = concurrency
	})
}

// WithInterval sets the interval between updates.
func WithInterval(interval time.Duration) Parameter {
	return parameterFunc(func(p *parameters) {
		p.interval = interval
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel: zerolog.GlobalLevel(),
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.scheduler == nil {
		return nil, errors.New("no scheduler specified")
	}
	if parameters.blocksProvider == nil {
		return nil, errors.New("no blocks provider specified")
	}
	if parameters.transactionsProvider == nil {
		return nil, errors.New("no transactions provider specified")
	}
	if parameters.transactionStateDiffsProvider == nil {
		return nil, errors.New("no transaction state diffs provider specified")
	}
	if parameters.blockMEVsSetter == nil {
		return nil, errors.New("no block MEV setter specified")
	}
	if parameters.processConcurrency == 0 {
		return nil, errors.New("no process concurrency specified")
	}
	if parameters.interval == 0 {
		return nil, errors.New("no interval specified")
	}

	return &parameters, nil
}
