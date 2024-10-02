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
func (*Service) BeginTx(_ context.Context) (context.Context, context.CancelFunc, error) {
	return nil, nil, nil
}

// CommitTx is a mock.
func (*Service) CommitTx(_ context.Context) error {
	return nil
}

// SetMetadata sets a metadata key to a JSON value.
func (*Service) SetMetadata(_ context.Context, _ string, _ []byte) error {
	return nil
}

// Metadata obtains the JSON value from a metadata key.
func (*Service) Metadata(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

// SetBlock is a mock.
func (*Service) SetBlock(_ context.Context, _ *execdb.Block) error {
	return nil
}

// SetBlocks sets multiple blocks efficiently.
func (*Service) SetBlocks(_ context.Context, _ []*execdb.Block) error {
	return nil
}

// Events is a mock.
func (*Service) Events(_ context.Context, _ *execdb.EventFilter) ([]*execdb.Event, error) {
	return nil, nil
}

// SetEvent is a mock.
func (*Service) SetEvent(_ context.Context, _ *execdb.Event) error {
	return nil
}

// SetEvents sets is a mock.
func (*Service) SetEvents(_ context.Context, _ []*execdb.Event) error {
	return nil
}

// Transactions is a mock.
func (*Service) Transactions(_ context.Context, _ *execdb.TransactionFilter) ([]*execdb.Transaction, error) {
	return nil, nil
}

// Transaction is a mock.
func (*Service) Transaction(_ context.Context, _ []byte) (*execdb.Transaction, error) {
	return nil, nil
}

// SetTransaction is a mock.
func (*Service) SetTransaction(_ context.Context, _ *execdb.Transaction) error {
	return nil
}

// SetTransactions is a mock.
func (*Service) SetTransactions(_ context.Context, _ []*execdb.Transaction) error {
	return nil
}

// SetTransactionStateDiff is a mock.
func (*Service) SetTransactionStateDiff(_ context.Context, _ *execdb.TransactionStateDiff) error {
	return nil
}

// SetTransactionStateDiffs is a mock.
func (*Service) SetTransactionStateDiffs(_ context.Context, _ []*execdb.TransactionStateDiff) error {
	return nil
}
