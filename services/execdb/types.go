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
)

// Block holds information about a block.
type Block struct {
	Height          uint32
	Hash            []byte
	BaseFee         uint64
	Difficulty      uint64
	ExtraData       []byte
	GasLimit        uint32
	GasUsed         uint32
	FeeRecipient    []byte
	ParentHash      []byte
	Size            uint32
	StateRoot       []byte
	Timestamp       time.Time
	TotalDifficulty *big.Int
	Issuance        *big.Int
}

// Transaction holds information about a transaction.
type Transaction struct {
	AccessList           map[string][][]byte
	BlockHeight          uint32
	BlockHash            []byte
	Index                uint32
	Type                 uint64
	From                 []byte
	GasLimit             uint32
	GasPrice             *uint64
	GasUsed              uint32
	Hash                 []byte
	Input                []byte
	MaxFeePerGas         *uint64
	MaxPriorityFeePerGas *uint64
	Nonce                uint64
	R                    *big.Int
	S                    *big.Int
	Status               uint32
	To                   []byte
	V                    *big.Int
	Value                *big.Int
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
