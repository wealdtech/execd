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

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// SetBlockReward sets a block's reward.
func (s *Service) SetBlockReward(ctx context.Context, reward *execdb.BlockReward) error {
	if reward == nil {
		return errors.New("block reward nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_block_rewards(f_block_hash
                           ,f_block_height
                           ,f_fees
                           ,f_payments
                           )
VALUES($1,$2,$3,$4)
ON CONFLICT (f_block_hash) DO
UPDATE
SET f_block_height = excluded.f_block_height
   ,f_fees = excluded.f_fees
   ,f_payments = excluded.f_payments
`,
		reward.BlockHash,
		reward.BlockHeight,
		decimal.NewFromBigInt(reward.Fees.ToBig(), 0),
		decimal.NewFromBigInt(reward.Payments.ToBig(), 0),
	)

	return err
}
