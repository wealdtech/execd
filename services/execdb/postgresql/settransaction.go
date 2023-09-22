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
	"encoding/hex"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetTransaction sets a transaction.
func (s *Service) SetTransaction(ctx context.Context, transaction *execdb.Transaction) error {
	if transaction == nil {
		return errors.New("transaction nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var input *[]byte
	if len(transaction.Input) > 0 {
		input = &transaction.Input
	}

	// Bytes() returns an empty string for 0x00, so need to work around that here.
	v := transaction.V.Bytes()
	if len(v) == 0 {
		v = []byte{0x00}
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_transactions(f_block_height
                          ,f_block_hash
                          ,f_contract_address
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
                          )
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
ON CONFLICT (f_block_hash,f_index) DO
UPDATE
SET f_block_height = excluded.f_block_height
   ,f_contract_address = excluded.f_contract_address
   ,f_type = excluded.f_type
   ,f_from = excluded.f_from
   ,f_gas_limit = excluded.f_gas_limit
   ,f_gas_price = excluded.f_gas_price
   ,f_gas_used = excluded.f_gas_used
   ,f_hash = excluded.f_hash
   ,f_input = excluded.f_input
   ,f_max_fee_per_gas = excluded.f_max_fee_per_gas
   ,f_max_priority_fee_per_gas = excluded.f_max_priority_fee_per_gas
   ,f_nonce = excluded.f_nonce
   ,f_r = excluded.f_r
   ,f_s = excluded.f_s
   ,f_status = excluded.f_status
   ,f_to = excluded.f_to
   ,f_v = excluded.f_v
   ,f_value = excluded.f_value
`,
		transaction.BlockHeight,
		transaction.BlockHash,
		transaction.ContractAddress,
		transaction.Index,
		transaction.Type,
		transaction.From,
		transaction.GasLimit,
		transaction.GasPrice,
		transaction.GasUsed,
		transaction.Hash,
		input,
		transaction.MaxFeePerGas,
		transaction.MaxPriorityFeePerGas,
		transaction.Nonce,
		transaction.R.Bytes(),
		transaction.S.Bytes(),
		transaction.Status,
		transaction.To,
		v,
		decimal.NewFromBigInt(transaction.Value, 0),
	)
	if err != nil {
		return err
	}

	for k, storageKeys := range transaction.AccessList {
		address, err := hex.DecodeString(k)
		if err != nil {
			// N.B. this should never happen, as we control encoding of the access list.
			return errors.Wrap(err, "failed to parse access list addredd")
		}
		_, err = tx.Exec(ctx, `
INSERT INTO t_transaction_access_lists(f_transaction_hash
                                      ,f_block_height
                                      ,f_address
                                      ,f_storage_keys
                                      )
VALUES($1,$2,$3,$4)
ON CONFLICT (f_transaction_hash,f_block_height,f_address) DO
UPDATE
SET f_storage_keys = excluded.f_storage_keys
`,
			transaction.Hash,
			transaction.BlockHeight,
			address,
			storageKeys,
		)
		if err != nil {
			return err
		}
	}
	return err
}
