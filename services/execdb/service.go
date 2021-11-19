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

// BlocksSetter defines functions to create and update blocks.
type BlocksSetter interface {
	Service
	// SetBlock sets a block.
	SetBlock(ctx context.Context, block *Block) error

	// SetBlocks sets multiple blocks efficiently.
	SetBlocks(ctx context.Context, blocks []*Block) error
}

// EventsSetter defines functions to create and update events.
type EventsSetter interface {
	Service
	// SetEvent sets an event.
	SetEvent(ctx context.Context, event *Event) error

	// SetEvents sets multiple events efficiently.
	SetEvents(ctx context.Context, events []*Event) error
}

// TransactionsSetter defines functions to create and update transactions.
type TransactionsSetter interface {
	Service
	// SetTransaction sets a transaction.
	SetTransaction(ctx context.Context, transaction *Transaction) error

	// SetTransaction sets multiple transactions efficiently.
	SetTransactions(ctx context.Context, transactions []*Transaction) error
}

// TransactionStateDiffsSetter defines functions to create and update state differences.
type TransactionStateDiffsSetter interface {
	Service
	// SetTransactionStateDiff sets a transaction's state differences.
	SetTransactionStateDiff(ctx context.Context, stateDiff *TransactionStateDiff) error

	// SetTransactionStateDiff sets multiple transactions' state differences efficiently.
	SetTransactionStateDiffs(ctx context.Context, stateDiffs []*TransactionStateDiff) error
}
