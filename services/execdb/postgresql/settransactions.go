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

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetTransactions sets multiple transactions efficiently
func (s *Service) SetTransactions(ctx context.Context, transactions []*execdb.Transaction) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Create a savepoint in case the copy fails.
	nestedTx, err := tx.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create nested transaction")
	}

	// Build the transaction access list.
	accessLists := make([]*execdb.TransactionAccessListEntry, 0)
	for _, transaction := range transactions {
		for k, v := range transaction.AccessList {
			key, err := hex.DecodeString(k)
			if err != nil {
				// N.B. this should never happen, as we control encoding of the access list.
				return errors.Wrap(err, "failed to parse access list addredd")
			}
			accessLists = append(accessLists, &execdb.TransactionAccessListEntry{
				TransactionHash: transaction.Hash,
				BlockHeight:     transaction.BlockHeight,
				Address:         key,
				StorageKeys:     v,
			})
		}
	}
	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_transactions"},
		[]string{
			"f_block_height",
			"f_block_hash",
			"f_contract_address",
			"f_index",
			"f_type",
			"f_from",
			"f_gas_limit",
			"f_gas_price",
			"f_gas_used",
			"f_hash",
			"f_input",
			"f_max_fee_per_gas",
			"f_max_priority_fee_per_gas",
			"f_nonce",
			"f_r",
			"f_s",
			"f_status",
			"f_to",
			"f_v",
			"f_value",
		},
		pgx.CopyFromSlice(len(accessLists), func(i int) ([]interface{}, error) {
			var input *[]byte
			if len(transactions[i].Input) > 0 {
				input = &transactions[i].Input
			}

			return []interface{}{
				transactions[i].BlockHeight,
				transactions[i].BlockHash,
				transactions[i].ContractAddress,
				transactions[i].Index,
				transactions[i].Type,
				transactions[i].From,
				transactions[i].GasLimit,
				transactions[i].GasPrice,
				transactions[i].GasUsed,
				transactions[i].Hash,
				input,
				transactions[i].MaxFeePerGas,
				transactions[i].MaxPriorityFeePerGas,
				transactions[i].Nonce,
				transactions[i].R.Bytes(),
				transactions[i].S.Bytes(),
				transactions[i].Status,
				transactions[i].To,
				transactions[i].V.Bytes(),
				decimal.NewFromBigInt(transactions[i].Value, 0),
			}, nil
		}))

	if err == nil {
		_, err = nestedTx.CopyFrom(ctx,
			pgx.Identifier{"t_transaction_access_lists"},
			[]string{
				"f_transaction_hash",
				"f_block_height",
				"f_address",
				"f_storage_keys",
			},
			pgx.CopyFromSlice(len(accessLists), func(i int) ([]interface{}, error) {
				return []interface{}{
					accessLists[i].TransactionHash,
					accessLists[i].BlockHeight,
					accessLists[i].Address,
					accessLists[i].StorageKeys,
				}, nil
			}))
		if err == nil {
			if err := nestedTx.Commit(ctx); err != nil {
				return errors.Wrap(err, "failed to commit nested transaction")
			}
		}
	}

	if err != nil {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert transactions; applying one at a time")
		for _, transaction := range transactions {
			if err := s.SetTransaction(ctx, transaction); err != nil {
				return err
			}
		}
	}

	return nil
}
