// Copyright © 2021 Weald Technology Limited.
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

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetTransactionStateDiffs sets multiple transactions' state differences efficiently.
func (s *Service) SetTransactionStateDiffs(ctx context.Context, stateDiffs []*execdb.TransactionStateDiff) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Flatten the balance changes.
	balanceChanges := make([]*execdb.TransactionBalanceChange, 0)
	for _, stateDiff := range stateDiffs {
		balanceChanges = append(balanceChanges, stateDiff.BalanceChanges...)
	}

	// Create a savepoint in case the copy fails.
	nestedTx, err := tx.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create nested transaction")
	}

	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_transaction_balance_changes"},
		[]string{
			"f_transaction_hash",
			"f_block_height",
			"f_address",
			"f_old",
			"f_new",
		},
		pgx.CopyFromSlice(len(balanceChanges), func(i int) ([]interface{}, error) {
			oldBalance := decimal.Zero
			if balanceChanges[i].Old != nil {
				oldBalance = decimal.NewFromBigInt(balanceChanges[i].Old, 0)
			}
			newBalance := decimal.Zero
			if balanceChanges[i].New != nil {
				newBalance = decimal.NewFromBigInt(balanceChanges[i].New, 0)
			}
			return []interface{}{
				balanceChanges[i].TransactionHash,
				balanceChanges[i].BlockHeight,
				balanceChanges[i].Address,
				oldBalance,
				newBalance,
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

		log.Debug().Err(err).Msg("Failed to copy insert state diffs; applying one at a time")
		for _, stateDiff := range stateDiffs {
			if err := s.SetTransactionStateDiff(ctx, stateDiff); err != nil {
				return err
			}
		}
	}

	return nil
}
