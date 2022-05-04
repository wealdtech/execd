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
	"errors"
	"time"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/attestantio/go-execution-client/types"
	"github.com/rs/zerolog"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/metrics"
	"github.com/wealdtech/execd/services/scheduler"
)

type parameters struct {
	logLevel            zerolog.Level
	monitor             metrics.Service
	scheduler           scheduler.Service
	chainHeightProvider execclient.ChainHeightProvider
	balancesProvider    execclient.BalancesProvider
	blocksProvider      execclient.BlocksProvider
	balancesSetter      execdb.BalancesSetter
	dbBalancesProvider  execdb.BalancesProvider
	trackDistance       uint32
	addresses           []types.Address
	startHeight         int64
	processConcurrency  int64
	interval            time.Duration
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

// WithChainHeightProvider sets the chain height provider for this module.
func WithChainHeightProvider(provider execclient.ChainHeightProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainHeightProvider = provider
	})
}

// WithBalancesProvider sets the balances provider for this module.
func WithBalancesProvider(provider execclient.BalancesProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.balancesProvider = provider
	})
}

// WithBlocksProvider sets the blocks provider for this module.
func WithBlocksProvider(provider execclient.BlocksProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blocksProvider = provider
	})
}

// WithBalancesSetter sets the balances setter for this module.
func WithBalancesSetter(setter execdb.BalancesSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.balancesSetter = setter
	})
}

// WithDBBalancesProvider sets the database balances provider for this module.
func WithDBBalancesProvider(provider execdb.BalancesProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.dbBalancesProvider = provider
	})
}

// WithTrackDistance sets the track distance for this module.
func WithTrackDistance(trackDistance uint32) Parameter {
	return parameterFunc(func(p *parameters) {
		p.trackDistance = trackDistance
	})
}

// WithAddresses sets the addresses for this module.
func WithAddresses(addresses []types.Address) Parameter {
	return parameterFunc(func(p *parameters) {
		p.addresses = addresses
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
	if parameters.chainHeightProvider == nil {
		return nil, errors.New("no chain height provider specified")
	}
	if parameters.balancesProvider == nil {
		return nil, errors.New("no balances provider specified")
	}
	if parameters.blocksProvider == nil {
		return nil, errors.New("no blocks provider specified")
	}
	if parameters.balancesSetter == nil {
		return nil, errors.New("no balances setter specified")
	}
	if parameters.dbBalancesProvider == nil {
		return nil, errors.New("no database balances provider specified")
	}
	if parameters.trackDistance == 0 {
		return nil, errors.New("no track distance specified")
	}
	if len(parameters.addresses) == 0 {
		return nil, errors.New("no addresses specified")
	}
	if parameters.processConcurrency == 0 {
		return nil, errors.New("no process concurrency specified")
	}
	if parameters.interval == 0 {
		return nil, errors.New("no interval specified")
	}

	return &parameters, nil
}
