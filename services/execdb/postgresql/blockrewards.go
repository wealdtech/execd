// Copyright © 2024 Weald Technology Trading.
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
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
)

// BlockRewards returns block rewards matching the supplied filter.
func (s *Service) BlockRewards(ctx context.Context, filter *execdb.BlockRewardFilter) ([]*execdb.BlockReward, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]interface{}, 0)

	queryBuilder.WriteString(`
SELECT f_block_height
      ,f_block_hash
      ,f_fees
      ,f_payments
FROM t_block_rewards`)

	conditions := make([]string, 0)

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		conditions = append(conditions, fmt.Sprintf("f_block_height >= $%d", len(queryVals)))
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		conditions = append(conditions, fmt.Sprintf("f_block_height <= $%d", len(queryVals)))
	}

	if filter.TimestampFrom != nil || filter.TimestampTo != nil {
		queryBuilder.WriteString(`
LEFT JOIN t_blocks ON t_blocks.f_hash = t_block_rewards.f_block_hash`)
	}

	if filter.TimestampFrom != nil {
		queryVals = append(queryVals, *filter.TimestampFrom)
		conditions = append(conditions, fmt.Sprintf("f_timestamp >= $%d", len(queryVals)))
	}

	if filter.TimestampTo != nil {
		queryVals = append(queryVals, *filter.TimestampTo)
		conditions = append(conditions, fmt.Sprintf("f_timestamp <= $%d", len(queryVals)))
	}

	if len(conditions) > 0 {
		queryBuilder.WriteString("\nWHERE ")
		queryBuilder.WriteString(strings.Join(conditions, "\n  AND "))
	}

	switch filter.Order {
	case execdb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_height`)
	case execdb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_height DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit != 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blockRewards := make([]*execdb.BlockReward, 0)
	for rows.Next() {
		blockReward := &execdb.BlockReward{}
		err := rows.Scan(
			&blockReward.BlockHeight,
			&blockReward.BlockHash,
			&blockReward.Fees,
			&blockReward.Payments,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		blockRewards = append(blockRewards, blockReward)
	}

	// Always return in order of height.
	sort.Slice(blockRewards, func(i int, j int) bool {
		return blockRewards[i].BlockHeight < blockRewards[j].BlockHeight
	})

	return blockRewards, nil
}
