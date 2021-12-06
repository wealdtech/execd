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

package mock

import (
	"context"

	"github.com/wealdtech/execd/services/execdb"
)

// Service is a mock.
type Service struct{}

// BeginTx is a mock.
func (s *Service) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return nil, nil, nil
}

// CommitTx is a mock.
func (s *Service) CommitTx(ctx context.Context) error {
	return nil
}

// SetMetadata sets a metadata key to a JSON value.
func (s *Service) SetMetadata(ctx context.Context, key string, value []byte) error {
	return nil
}

// Metadata obtains the JSON value from a metadata key.
func (s *Service) Metadata(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}

// SetBlock is a mock.
func (s *Service) SetBlock(ctx context.Context, block *execdb.Block) error {
	return nil
}

// SetBlocks sets multiple blocks efficiently.
func (s *Service) SetBlocks(ctx context.Context, blocks []*execdb.Block) error {
	return nil
}

// Events is a mock.
func (s *Service) Events(ctx context.Context, filter *execdb.EventFilter) ([]*execdb.Event, error) {
	return nil, nil
}

// SetEvent is a mock.
func (s *Service) SetEvent(ctx context.Context, event *execdb.Event) error {
	return nil
}

// SetEvents sets is a mock.
func (s *Service) SetEvents(ctx context.Context, events []*execdb.Event) error {
	return nil
}

// Transactions is a mock.
func (s *Service) Transactions(ctx context.Context, filter *execdb.TransactionFilter) ([]*execdb.Transaction, error) {
	return nil, nil
}

// SetTransaction is a mock.
func (s *Service) SetTransaction(ctx context.Context, transaction *execdb.Transaction) error {
	return nil
}

// SetTransactions is a mock.
func (s *Service) SetTransactions(ctx context.Context, transactions []*execdb.Transaction) error {
	return nil
}

// SetTransactionStateDiff is a mock.
func (s *Service) SetTransactionStateDiff(ctx context.Context, stateDiff *execdb.TransactionStateDiff) error {
	return nil
}

// SetTransactionStateDiffs is a mock.
func (s *Service) SetTransactionStateDiffs(ctx context.Context, stateDiffs []*execdb.TransactionStateDiff) error {
	return nil
}
