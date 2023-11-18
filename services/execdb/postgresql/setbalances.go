// Copyright © 2022 Weald Technology Trading.
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
	"github.com/wealdtech/execd/services/execdb"
)

// SetBalances sets multiple balances efficiently.
func (s *Service) SetBalances(ctx context.Context, balances []*execdb.Balance) error {
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
		pgx.Identifier{"t_balances"},
		[]string{
			"f_address",
			"f_currency",
			"f_from",
			"f_amount",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]interface{}, error) {
			return []interface{}{
				balances[i].Address[:],
				balances[i].Currency,
				balances[i].From,
				balances[i].Amount,
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

		log.Debug().Err(err).Msg("Failed to copy insert balances; applying one at a time")
		for _, balance := range balances {
			if err := s.SetBalance(ctx, balance); err != nil {
				return err
			}
		}
	}

	return nil
}
