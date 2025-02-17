// Copyright © 2021 - 2025 Weald Technology Trading.
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

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetTransactions sets multiple transactions efficiently.
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
			"f_y_parity",
			"f_max_fee_per_blob_gas",
			"f_blob_versioned_hashes",
			"f_blob_gas_price",
			"f_blob_gas_used",
		},
		pgx.CopyFromSlice(len(transactions), func(i int) ([]any, error) {
			var input *[]byte
			if len(transactions[i].Input) > 0 {
				input = &transactions[i].Input
			}

			// Bytes() returns an empty string for 0x00, so need to work around that here.
			v := transactions[i].V.Bytes()
			if len(v) == 0 {
				v = []byte{0x00}
			}

			var blobGasUsed *decimal.Decimal
			if transactions[i].BlobGasPrice != nil {
				dec := decimal.NewFromBigInt(transactions[i].BlobGasPrice, 0)
				blobGasUsed = &dec
			}

			return []any{
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
				v,
				decimal.NewFromBigInt(transactions[i].Value, 0),
				transactions[i].YParity,
				transactions[i].MaxFeePerBlobGas,
				transactions[i].BlobVersionedHashes,
				blobGasUsed,
				transactions[i].BlobGasUsed,
			}, nil
		}))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to copy insert transactions; applying one at a time")

		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		for _, transaction := range transactions {
			if err := s.SetTransaction(ctx, transaction); err != nil {
				return err
			}
		}

		return nil
	}

	// Build the transaction access list.
	accessLists := make([]*execdb.TransactionAccessListEntry, 0)
	for _, transaction := range transactions {
		for k, v := range transaction.AccessList {
			key, err := hex.DecodeString(k)
			if err != nil {
				// N.B. this should never happen, as we control encoding of the access list.
				return errors.Wrap(err, "failed to parse access list address")
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
		pgx.Identifier{"t_transaction_access_lists"},
		[]string{
			"f_transaction_hash",
			"f_block_height",
			"f_address",
			"f_storage_keys",
		},
		pgx.CopyFromSlice(len(accessLists), func(i int) ([]any, error) {
			return []any{
				accessLists[i].TransactionHash,
				accessLists[i].BlockHeight,
				accessLists[i].Address,
				accessLists[i].StorageKeys,
			}, nil
		}))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to copy insert transaction access lists; applying transactions one at a time")

		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		for _, transaction := range transactions {
			if err := s.SetTransaction(ctx, transaction); err != nil {
				return err
			}
		}

		return nil
	}

	// Build the transaction authorization list aggregate.
	authorizationListEntries := make([]*execdb.TransactionAuthorizationListEntry, 0)
	for _, transaction := range transactions {
		authorizationListEntries = append(authorizationListEntries, transaction.AuthorizationList...)
	}

	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_transaction_authorization_lists"},
		[]string{
			"f_transaction_hash",
			"f_block_height",
			"f_index",
			"f_chain_id",
			"f_address",
			"f_nonce",
			"f_r",
			"f_s",
			"f_y_parity",
		},
		pgx.CopyFromSlice(len(authorizationListEntries), func(i int) ([]any, error) {
			return []any{
				authorizationListEntries[i].TransactionHash,
				authorizationListEntries[i].BlockHeight,
				authorizationListEntries[i].Index,
				authorizationListEntries[i].ChainID,
				authorizationListEntries[i].Address,
				authorizationListEntries[i].Nonce,
				authorizationListEntries[i].R.Bytes(),
				authorizationListEntries[i].S.Bytes(),
				authorizationListEntries[i].YParity,
			}, nil
		}))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to copy insert transaction authorization lists; applying transactions one at a time")

		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		for _, transaction := range transactions {
			if err := s.SetTransaction(ctx, transaction); err != nil {
				return err
			}
		}

		return nil
	}

	if err := nestedTx.Commit(ctx); err != nil {
		return errors.Wrap(err, "failed to commit nested transaction")
	}

	return nil
}
