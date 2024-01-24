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

package postgresql

import (
	"context"
	"math/big"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// Transaction returns the transaction matching the supplied hash.
func (s *Service) Transaction(ctx context.Context, hash []byte) (*execdb.Transaction, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	transaction := &execdb.Transaction{}
	sigR := make([]byte, 0)
	sigS := make([]byte, 0)
	sigV := make([]byte, 0)
	value := decimal.Zero
	err := tx.QueryRow(ctx, `
SELECT f_block_height
      ,f_block_hash
      ,f_index
      ,f_type
      ,f_from
      ,f_gas_limit
      ,f_gas_price
      ,f_gas_used
      ,f_hash
      ,f_input
      ,f_max_fee_per_gas
      ,f_max_priority_fee_per_gas
      ,f_nonce
      ,f_r
      ,f_s
      ,f_status
      ,f_to
      ,f_v
      ,f_value
	  ,f_y_parity
	  ,f_max_fee_per_blob_gas
	  ,f_blob_versioned_hashes
	  ,f_blob_gas_used
FROM t_transactions
WHERE f_hash = $1`,
		hash).Scan(
		&transaction.BlockHeight,
		&transaction.BlockHash,
		&transaction.Index,
		&transaction.Type,
		&transaction.From,
		&transaction.GasLimit,
		&transaction.GasPrice,
		&transaction.GasUsed,
		&transaction.Hash,
		&transaction.Input,
		&transaction.MaxFeePerGas,
		&transaction.MaxPriorityFeePerGas,
		&transaction.Nonce,
		&sigR,
		&sigS,
		&transaction.Status,
		&transaction.To,
		&sigV,
		&value,
		&transaction.YParity,
		&transaction.MaxFeePerBlobGas,
		&transaction.BlobVersionedHashes,
		&transaction.BlobGasUsed,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}
	transaction.R = new(big.Int).SetBytes(sigR)
	transaction.S = new(big.Int).SetBytes(sigS)
	transaction.V = new(big.Int).SetBytes(sigV)
	var success bool
	transaction.Value, success = new(big.Int).SetString(value.String(), 10)
	if !success {
		return nil, errors.New("Failed to obtain value")
	}

	return transaction, nil
}
