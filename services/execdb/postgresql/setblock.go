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

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetBlock sets a block.
func (s *Service) SetBlock(ctx context.Context, block *execdb.Block) error {
	if block == nil {
		return errors.New("block nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var issuance *decimal.Decimal
	if block.Issuance != nil {
		tmp := decimal.NewFromBigInt(block.Issuance, 0)
		issuance = &tmp
	}
	var withdrawalsRoot []byte
	if len(block.WithdrawalsRoot) > 0 {
		withdrawalsRoot = block.WithdrawalsRoot
	}
	var parentBeaconBlockRoot []byte
	if len(block.ParentBeaconBlockRoot) > 0 {
		parentBeaconBlockRoot = block.ParentBeaconBlockRoot
	}
	var totalDifficulty *decimal.Decimal
	if block.TotalDifficulty != nil {
		tmp := decimal.NewFromBigInt(block.TotalDifficulty, 0)
		totalDifficulty = &tmp
	}
	var requestsHash []byte
	if len(block.RequestsHash) > 0 {
		requestsHash = block.RequestsHash
	}
	_, err := tx.Exec(ctx, `
INSERT INTO t_blocks(f_height
                    ,f_hash
                    ,f_base_fee
                    ,f_difficulty
                    ,f_extra_data
                    ,f_fee_recipient
                    ,f_gas_limit
                    ,f_gas_used
                    ,f_parent_hash
                    ,f_size
                    ,f_state_root
                    ,f_timestamp
                    ,f_total_difficulty
                    ,f_issuance
                    ,f_withdrawals_root
                    ,f_parent_beacon_block_root
                    ,f_blob_gas_used
                    ,f_excess_blob_gas
                    ,f_requests_hash
                    )
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT (f_hash) DO
UPDATE
SET f_height = excluded.f_height
   ,f_base_fee = excluded.f_base_fee
   ,f_difficulty = excluded.f_difficulty
   ,f_extra_data = excluded.f_extra_data
   ,f_fee_recipient = excluded.f_fee_recipient
   ,f_gas_limit = excluded.f_gas_limit
   ,f_gas_used = excluded.f_gas_used
   ,f_parent_hash = excluded.f_parent_hash
   ,f_size = excluded.f_size
   ,f_state_root = excluded.f_state_root
   ,f_timestamp = excluded.f_timestamp
   ,f_total_difficulty = excluded.f_total_difficulty
   ,f_issuance = excluded.f_issuance
   ,f_withdrawals_root = excluded.f_withdrawals_root
   ,f_parent_beacon_block_root = excluded.f_parent_beacon_block_root
   ,f_blob_gas_used = excluded.f_blob_gas_used
   ,f_excess_blob_gas = excluded.f_excess_blob_gas
   ,f_requests_hash = excluded.f_requests_hash
`,
		block.Height,
		block.Hash,
		block.BaseFee,
		block.Difficulty,
		block.ExtraData,
		block.FeeRecipient,
		block.GasLimit,
		block.GasUsed,
		block.ParentHash,
		block.Size,
		block.StateRoot,
		block.Timestamp,
		totalDifficulty,
		issuance,
		withdrawalsRoot,
		parentBeaconBlockRoot,
		block.BlobGasUsed,
		block.ExcessBlobGas,
		requestsHash,
	)

	return err
}
