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
	"time"
)

// Order is the order in which results should be fetched (N.B. fetched, not returned).
type Order uint8

const (
	// OrderEarliest fetches earliest transactions first.
	OrderEarliest Order = iota
	// OrderLatest fetches latest transactions first.
	OrderLatest
)

// BalanceFilter defines a filter for fetching balances.
// Filter elements are ANDed together.
// Results are always returned in ascending holder/timestamp/currency order.
type BalanceFilter struct {
	// Limit is the maximum number of balances to return.
	// If 0 then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// Currency is the currency to fetch.
	// If nil then there is no currency filter
	Currency string

	// From is the timestamp of the earliest balance to fetch.
	// If nil then there is no earliest balance.
	From *time.Time

	// To is the timestamp of the latest balance to fetch.
	// If nil then there is no latest balance.
	To *time.Time

	// Holders are the holders of the balance.
	// If nil then no filter is applied.
	Holders [][]byte
}

// TransactionFilter defines a filter for fetching transactions.
// Filter elements are ANDed together.
// Results are always returned in ascending (block height, transaction index) order.
type TransactionFilter struct {
	// Limit is the maximum number of transactions to return.
	// If 0 then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the height of the earliest block from which to fetch transactions.
	// If nil then there is no earliest block.
	From *uint32

	// To is the height of the latest block from which to fetch transactions.
	// If nil then there is no latest block.
	To *uint32

	// Sender is the address of the sender field in the transaction.
	// If nil then no filter is applied
	Sender *[]byte

	// Recipient is the address of the recipient field in the transaction.
	// If nil then no filter is applied
	Recipient *[]byte
}

// BlockFilter defines a filter for fetching blocks.
// Filter elements are ANDed together.
// Results are always returned in ascending block height order.
type BlockFilter struct {
	// Limit is the maximum number of blocks to return.
	// If 0 then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the height of the earliest block to fetch.
	// If nil then there is no earliest block.
	From *uint32

	// TimestampTo is the height of the latest block to fetch.
	// If nil then there is no latest block.
	To *uint32

	// From is the timestamp of the earliest block to fetch.
	// If nil then there is no earliest block.
	TimestampFrom *time.Time

	// TimestampTo is the timestamp of the latest block to fetch.
	// If nil then there is no latest block.
	TimestampTo *time.Time

	// FeeRecipients are the fee recipients of the blocks.
	// If nil then there is no fee recipients filter.
	FeeRecipients *[][]byte
}

// BlockRewardFilter defines a filter for fetching block rewards.
// Filter elements are ANDed together.
// Results are always returned in ascending block height order.
type BlockRewardFilter struct {
	// Limit is the maximum number of block rewards to return.
	// If 0 then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the height of the earliest block reward to fetch.
	// If nil then there is no earliest block.
	From *uint32

	// TimestampTo is the height of the latest block reward to fetch.
	// If nil then there is no latest block.
	To *uint32

	// From is the timestamp of the earliest block reward to fetch.
	// If nil then there is no earliest block.
	TimestampFrom *time.Time

	// TimestampTo is the timestamp of the latest block reward to fetch.
	// If nil then there is no latest block.
	TimestampTo *time.Time
}

// EventFilter defines a filter for fetching events.
// Filter elements are ANDed together.
// Results are always returned in ascending (block height, transaction index, event index) order.
type EventFilter struct {
	// Limit is the maximum number of events to return.
	// If zero then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the height of the earliest block from which to fetch events.
	// If nil then there is no earliest block.
	From *uint32

	// To is the height of the latest block from which to fetch events.
	// If nil then there is no latest block.
	To *uint32

	// TransactionHash is the hash of the transaction that generated the event.
	// If nil then no filter is applied
	TransactionHash *[]byte

	// Address is the address of the contract that generated the event.
	// If nil then no filter is applied
	Address *[]byte

	// Topics are the topics of the event.
	// If nil then no filter is applied.
	Topics [][]byte
}

// TransactionBalanceChangeFilter defines a filter for fetching transaction balance changes.
// Filter elements are ANDed together.
// Results are always returned in ascending (block height, transaction index) order.
type TransactionBalanceChangeFilter struct {
	// Limit is the maximum number of transactions to return.
	// If zero then there is no limit.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the height of the earliest block from which to fetch transactions.
	// If nil then there is no earliest block.
	From *uint32

	// To is the height of the latest block from which to fetch transactions.
	// If nil then there is no latest block.
	To *uint32

	// TxHashes are the transaction hashes for which to obtain balance changes.
	// If empty then no filter is applied
	TxHashes [][]byte

	// Addresses are the address for which to obtain balance changes.
	// If empty then no filter is applied
	Addresses [][]byte
}
