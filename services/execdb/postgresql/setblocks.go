// Copyright © 2021, 2024 Weald Technology Trading.
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

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetBlocks sets multiple blocks efficiently.
func (s *Service) SetBlocks(ctx context.Context, blocks []*execdb.Block) error {
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
		pgx.Identifier{"t_blocks"},
		[]string{
			"f_height",
			"f_hash",
			"f_base_fee",
			"f_difficulty",
			"f_extra_data",
			"f_fee_recipient",
			"f_gas_limit",
			"f_gas_used",
			"f_parent_hash",
			"f_size",
			"f_state_root",
			"f_timestamp",
			"f_total_difficulty",
			"f_issuance",
			"f_withdrawals_root",
			"f_parent_beacon_block_root",
			"f_blob_gas_used",
			"f_excess_blob_gas",
		},
		pgx.CopyFromSlice(len(blocks), func(i int) ([]any, error) {
			var issuance *decimal.Decimal
			if blocks[i].Issuance != nil {
				tmp := decimal.NewFromBigInt(blocks[i].Issuance, 0)
				issuance = &tmp
			}
			var withdrawalsRoot []byte
			if len(blocks[i].WithdrawalsRoot) > 0 {
				withdrawalsRoot = blocks[i].WithdrawalsRoot
			}
			var parentBeaconBlockRoot []byte
			if len(blocks[i].ParentBeaconBlockRoot) > 0 {
				parentBeaconBlockRoot = blocks[i].ParentBeaconBlockRoot
			}
			return []any{
				blocks[i].Height,
				blocks[i].Hash,
				blocks[i].BaseFee,
				blocks[i].Difficulty,
				blocks[i].ExtraData,
				blocks[i].FeeRecipient,
				blocks[i].GasLimit,
				blocks[i].GasUsed,
				blocks[i].ParentHash,
				blocks[i].Size,
				blocks[i].StateRoot,
				blocks[i].Timestamp,
				decimal.NewFromBigInt(blocks[i].TotalDifficulty, 0),
				issuance,
				withdrawalsRoot,
				parentBeaconBlockRoot,
				blocks[i].BlobGasUsed,
				blocks[i].ExcessBlobGas,
			}, nil
		}))

	if err == nil {
		if err := nestedTx.Commit(ctx); err != nil {
			return errors.Wrap(err, "failed to commit nested transaction")
		}
	} else {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert blocks; applying one at a time")
		for _, block := range blocks {
			if err := s.SetBlock(ctx, block); err != nil {
				return err
			}
		}
	}

	return nil
}
