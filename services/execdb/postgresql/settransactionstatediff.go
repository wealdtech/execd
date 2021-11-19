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

// SetTransactionStateDiff sets a transaction's state differences.
func (s *Service) SetTransactionStateDiff(ctx context.Context, stateDiff *execdb.TransactionStateDiff) error {
	if stateDiff == nil {
		return errors.New("state difference nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	for _, balanceChange := range stateDiff.BalanceChanges {
		oldBalance := decimal.Zero
		if balanceChange.Old != nil {
			oldBalance = decimal.NewFromBigInt(balanceChange.Old, 0)
		}
		newBalance := decimal.Zero
		if balanceChange.New != nil {
			newBalance = decimal.NewFromBigInt(balanceChange.New, 0)
		}
		_, err := tx.Exec(ctx, `
INSERT INTO t_transaction_balance_changes(f_transaction_hash
                                         ,f_block_height
                                         ,f_address
                                         ,f_old
                                         ,f_new
                                         )
VALUES($1,$2,$3,$4,$5)
ON CONFLICT (f_transaction_hash,f_block_height,f_address) DO
UPDATE
SET f_old = excluded.f_old
   ,f_new = excluded.f_new
`,
			balanceChange.TransactionHash,
			balanceChange.BlockHeight,
			balanceChange.Address,
			oldBalance,
			newBalance,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
