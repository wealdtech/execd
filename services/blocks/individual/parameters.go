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
	"errors"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/rs/zerolog"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/metrics"
)

type parameters struct {
	logLevel                    zerolog.Level
	monitor                     metrics.Service
	chainHeightProvider         execclient.ChainHeightProvider
	blocksProvider              execclient.BlocksProvider
	blockReplaysProvider        execclient.BlockReplaysProvider
	transactionReceiptsProvider execclient.TransactionReceiptsProvider
	blocksSetter                execdb.BlocksSetter
	transactionsSetter          execdb.TransactionsSetter
	transactionStateDiffsSetter execdb.TransactionStateDiffsSetter
	eventsSetter                execdb.EventsSetter
	startHeight                 int64
	enableTransactions          bool
	enableTransactionEvents     bool
	enableBalanceChanges        bool
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

// WithChainHeightProvider sets the chain height provider for this module.
func WithChainHeightProvider(provider execclient.ChainHeightProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainHeightProvider = provider
	})
}

// WithBlocksProvider sets the blocks provider for this module.
func WithBlocksProvider(provider execclient.BlocksProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blocksProvider = provider
	})
}

// WithBlockReplaysProvider sets the block replays provider for this module.
func WithBlockReplaysProvider(provider execclient.BlockReplaysProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blockReplaysProvider = provider
	})
}

// WithTransactionReceiptsProvider sets the tranasaction receipts provider for this module.
func WithTransactionReceiptsProvider(provider execclient.TransactionReceiptsProvider) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionReceiptsProvider = provider
	})
}

// WithBlocksSetter sets the blocks setter for this module.
func WithBlocksSetter(setter execdb.BlocksSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.blocksSetter = setter
	})
}

// WithTransactionsSetter sets the transactions setter for this module.
func WithTransactionsSetter(setter execdb.TransactionsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionsSetter = setter
	})
}

// WithTransactionStateDiffsSetter sets the block transactions changes setter for this module.
func WithTransactionStateDiffsSetter(setter execdb.TransactionStateDiffsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.transactionStateDiffsSetter = setter
	})
}

// WithEventsSetter sets the events setter for this module.
func WithEventsSetter(setter execdb.EventsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.eventsSetter = setter
	})
}

// WithStartHeight sets the start height for this module.
func WithStartHeight(startHeight int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startHeight = startHeight
	})
}

// WithTransactions sets the storage of transaction data.
func WithTransactions(enable bool) Parameter {
	return parameterFunc(func(p *parameters) {
		p.enableTransactions = enable
	})
}

// WithTransactionEvents sets the storage of transaction event data.
func WithTransactionEvents(enable bool) Parameter {
	return parameterFunc(func(p *parameters) {
		p.enableTransactionEvents = enable
	})
}

// WithBalanceChanges sets the storage of balance change data.
func WithBalanceChanges(enable bool) Parameter {
	return parameterFunc(func(p *parameters) {
		p.enableBalanceChanges = enable
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel:    zerolog.GlobalLevel(),
		startHeight: -1,
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.chainHeightProvider == nil {
		return nil, errors.New("no chain height provider specified")
	}
	if parameters.blocksProvider == nil {
		return nil, errors.New("no blocks provider specified")
	}
	if parameters.blockReplaysProvider == nil {
		return nil, errors.New("no block replays provider specified")
	}
	if parameters.transactionReceiptsProvider == nil {
		return nil, errors.New("no transaction receipts provider specified")
	}
	if parameters.blocksSetter == nil {
		return nil, errors.New("no blocks setter specified")
	}
	if parameters.transactionsSetter == nil {
		return nil, errors.New("no transactions setter specified")
	}
	if parameters.transactionStateDiffsSetter == nil {
		return nil, errors.New("no transaction state differences setter specified")
	}
	if parameters.eventsSetter == nil {
		return nil, errors.New("no events setter specified")
	}

	return &parameters, nil
}
