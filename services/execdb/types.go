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
	"math/big"
	"time"

	"github.com/attestantio/go-execution-client/types"
	"github.com/holiman/uint256"
	"github.com/shopspring/decimal"
)

// Balance holds information about a balance.
type Balance struct {
	Address  types.Address
	Currency string
	Amount   decimal.Decimal
	From     time.Time
}

// Block holds information about a block.
type Block struct {
	Height                uint32
	Hash                  []byte
	BaseFee               uint64
	Difficulty            uint64
	ExtraData             []byte
	GasLimit              uint32
	GasUsed               uint32
	FeeRecipient          []byte
	ParentBeaconBlockRoot []byte
	ParentHash            []byte
	Size                  uint32
	StateRoot             []byte
	WithdrawalsRoot       []byte
	Timestamp             time.Time
	TotalDifficulty       *big.Int
	Issuance              *big.Int
	BlobGasUsed           *uint64
	ExcessBlobGas         *uint64
	RequestsHash          []byte
}

// Transaction holds information about a transaction.
type Transaction struct {
	AccessList           map[string][][]byte
	AuthorizationList    []*TransactionAuthorizationListEntry
	BlockHeight          uint32
	BlockHash            []byte
	ContractAddress      *[]byte
	Index                uint32
	Type                 uint64
	From                 []byte
	GasLimit             uint32
	GasPrice             uint64
	GasUsed              uint32
	Hash                 []byte
	Input                []byte
	MaxFeePerGas         *uint64
	MaxPriorityFeePerGas *uint64
	MaxFeePerBlobGas     *uint64
	BlobGasPrice         *big.Int
	BlobGasUsed          *uint32
	BlobVersionedHashes  *[][]byte
	Nonce                uint64
	R                    *big.Int
	S                    *big.Int
	Status               uint32
	To                   *[]byte
	V                    *big.Int
	Value                *big.Int
	YParity              *bool
}

// TransactionAccessListEntry holds information about a transaction access list.
type TransactionAccessListEntry struct {
	TransactionHash []byte
	BlockHeight     uint32
	Address         []byte
	StorageKeys     [][]byte
}

// TransactionAuthorizationListEntry holds information about a transaction authorization list.
type TransactionAuthorizationListEntry struct {
	TransactionHash []byte
	BlockHeight     uint32
	Index           uint32
	ChainID         []byte
	Address         []byte
	Nonce           uint64
	R               *big.Int
	S               *big.Int
	YParity         bool
}

// TransactionStateDiff holds information about state differences as a result of a transaction.
type TransactionStateDiff struct {
	BalanceChanges []*TransactionBalanceChange
	StorageChanges []*TransactionStorageChange
}

// TransactionBalanceChange holds information about a balance change as a result of a transaction.
type TransactionBalanceChange struct {
	TransactionHash []byte
	BlockHeight     uint32
	Address         []byte
	Old             *big.Int
	New             *big.Int
}

// TransactionStorageChange holds information about a storage change as a result of a transaction.
type TransactionStorageChange struct {
	TransactionHash []byte
	BlockHeight     uint32
	Address         []byte
	StorageAddress  []byte
	Value           []byte
}

// Event holds information about a transaction event.
type Event struct {
	TransactionHash []byte
	BlockHeight     uint32
	Index           uint32 // aka LogIndex, index of the event in the block.
	Address         []byte
	Topics          [][]byte
	Data            []byte
}

// BlockReward holds information about the reward paid to the fee recipient in a block.
type BlockReward struct {
	BlockHash   []byte
	BlockHeight uint32
	Fees        *uint256.Int
	Payments    *uint256.Int
}
