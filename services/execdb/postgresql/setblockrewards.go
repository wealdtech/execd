// Copyright © 2021, 2022 Weald Technology Trading.
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

// SetBlockRewards sets multiple block rewards efficiently.
func (s *Service) SetBlockRewards(ctx context.Context, rewards []*execdb.BlockReward) error {
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
		pgx.Identifier{"t_block_rewards"},
		[]string{
			"f_block_hash",
			"f_block_height",
			"f_fees",
			"f_payments",
		},
		pgx.CopyFromSlice(len(rewards), func(i int) ([]any, error) {
			return []any{
				rewards[i].BlockHash,
				rewards[i].BlockHeight,
				decimal.NewFromBigInt(rewards[i].Fees.ToBig(), 0),
				decimal.NewFromBigInt(rewards[i].Payments.ToBig(), 0),
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
		for _, reward := range rewards {
			if err := s.SetBlockReward(ctx, reward); err != nil {
				return err
			}
		}
	}

	return nil
}
