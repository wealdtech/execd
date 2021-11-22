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

// Order is the order in which results should be fetched (N.B. fetched, not returned).
type Order uint8

const (
	// OrderUnknown is an unknown order.
	OrderUnknown Order = iota
	// OrderEarliest fetches earliest transactions first.
	OrderEarliest
	// OrderLatest fetches latest transactions first.
	OrderLatest
)

// TransactionFilter defines a filter for fetching transactions.
// Filter elements are ANDed together.
// Results are always returned in ascending (block height, index) order.
type TransactionFilter struct {
	// Limit is the maximum number of transactions to return.
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
