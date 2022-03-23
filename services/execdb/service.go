// Copyright © 2021, 2022 Weald Technology Trading.
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

package execdb

import (
	"context"
)

// Service defines a minimal exec database service.
type Service interface {
	// BeginTx begins a transaction.
	BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error)

	// CommitTx commits a transaction.
	CommitTx(ctx context.Context) error

	// SetMetadata sets a metadata key to a JSON value.
	SetMetadata(ctx context.Context, key string, value []byte) error

	// Metadata obtains the JSON value from a metadata key.
	Metadata(ctx context.Context, key string) ([]byte, error)
}

// BalancesProvider defines functions to provide balance information.
type BalancesProvider interface {
	// Balances returns balances matching the supplied filter.
	Balances(ctx context.Context, filter *BalanceFilter) ([]*Balance, error)
}

// BalancesSetter defines functions to create and update balances.
type BalancesSetter interface {
	Service
	// SetBalance sets a balance.
	SetBalance(ctx context.Context, balance *Balance) error

	// SetBalances sets multiple balances efficiently.
	SetBalances(ctx context.Context, balances []*Balance) error
}

// BlocksProvider defines functions to provide block information.
type BlocksProvider interface {
	// Blocks returns blocks matching the supplied filter.
	Blocks(ctx context.Context, filter *BlockFilter) ([]*Block, error)
}

// BlocksSetter defines functions to create and update blocks.
type BlocksSetter interface {
	Service
	// SetBlock sets a block.
	SetBlock(ctx context.Context, block *Block) error

	// SetBlocks sets multiple blocks efficiently.
	SetBlocks(ctx context.Context, blocks []*Block) error
}

// BlockMEVsSetter defines functions to create and update block MEV.
type BlockMEVsSetter interface {
	Service

	// SetBlockMEVs sets multiple block MEVs efficiently.
	SetBlockMEVs(ctx context.Context, blockMEVs []*BlockMEV) error
}

// EventsProvider defines functions to provide event information.
type EventsProvider interface {
	// Events returns events matching the supplied filter.
	Events(ctx context.Context, filter *EventFilter) ([]*Event, error)
}

// EventsSetter defines functions to create and update events.
type EventsSetter interface {
	Service

	// SetEvent sets an event.
	SetEvent(ctx context.Context, event *Event) error

	// SetEvents sets multiple events efficiently.
	SetEvents(ctx context.Context, events []*Event) error
}

// TransactionsProvider defines functions to provide transaction information.
type TransactionsProvider interface {
	// Transactions returns transactions matching the supplied filter.
	Transactions(ctx context.Context, filter *TransactionFilter) ([]*Transaction, error)

	// Transaction returns the transaction matching the supplied hash.
	Transaction(ctx context.Context, hash []byte) (*Transaction, error)
}

// TransactionsSetter defines functions to create and update transactions.
type TransactionsSetter interface {
	Service

	// SetTransaction sets a transaction.
	SetTransaction(ctx context.Context, transaction *Transaction) error

	// SetTransactions sets multiple transactions efficiently.
	SetTransactions(ctx context.Context, transactions []*Transaction) error
}

// TransactionStateDiffsProvider defines function to provide transaction state diff information.
type TransactionStateDiffsProvider interface {
	// TransactionStateDiff returns transaction state diffs for the supplied hash.
	TransactionStateDiff(ctx context.Context, hash []byte) (*TransactionStateDiff, error)
}

// TransactionStateDiffsSetter defines functions to create and update state differences.
type TransactionStateDiffsSetter interface {
	Service
	// SetTransactionStateDiff sets a transaction's state differences.
	SetTransactionStateDiff(ctx context.Context, stateDiff *TransactionStateDiff) error

	// SetTransactionStateDiffs sets multiple transactions' state differences efficiently.
	SetTransactionStateDiffs(ctx context.Context, stateDiffs []*TransactionStateDiff) error
}
