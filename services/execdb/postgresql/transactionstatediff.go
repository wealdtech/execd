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

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// TransactionStateDiff returns transaction state diffs for the supplied hash.
func (s *Service) TransactionStateDiff(ctx context.Context, hash []byte) (*execdb.TransactionStateDiff, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	rows, err := tx.Query(ctx, `
SELECT f_transaction_hash
      ,f_block_height
      ,f_address
      ,f_old
      ,f_new
FROM t_transaction_balance_changes
WHERE f_transaction_hash = $1`,
		hash,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stateDiff := &execdb.TransactionStateDiff{}
	for rows.Next() {
		balanceChange := &execdb.TransactionBalanceChange{}
		var oldBalance decimal.Decimal
		var newBalance decimal.Decimal
		err := rows.Scan(
			&balanceChange.TransactionHash,
			&balanceChange.BlockHeight,
			&balanceChange.Address,
			&oldBalance,
			&newBalance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		balanceChange.Old = oldBalance.BigInt()
		balanceChange.New = newBalance.BigInt()
		stateDiff.BalanceChanges = append(stateDiff.BalanceChanges, balanceChange)
	}

	return stateDiff, nil
}
