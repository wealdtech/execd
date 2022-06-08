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
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// Blocks returns blocks matching the supplied filter.
func (s *Service) Blocks(ctx context.Context, filter *execdb.BlockFilter) ([]*execdb.Block, error) {
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
SELECT f_height
      ,f_hash
      ,f_base_fee
      ,f_difficulty
      ,f_extra_data
      ,f_gas_limit
      ,f_gas_used
      ,f_fee_recipient
      ,f_parent_hash
      ,f_size
      ,f_state_root
      ,f_timestamp
      ,f_total_difficulty
      ,f_issuance
FROM t_blocks`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_height >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_height <= $%d`, wherestr, len(queryVals)))
	}

	if filter.TimestampFrom != nil {
		queryVals = append(queryVals, *filter.TimestampFrom)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_timestamp >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.TimestampTo != nil {
		queryVals = append(queryVals, *filter.TimestampTo)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_timestamp <= $%d`, wherestr, len(queryVals)))
	}

	if filter.FeeRecipients != nil {
		queryVals = append(queryVals, *filter.FeeRecipients)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_fee_recipient = ANY($%d)`, wherestr, len(queryVals)))
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

	if filter.Limit != nil {
		queryVals = append(queryVals, *filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		log.Trace().Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*execdb.Block, 0)
	var totalDifficulty decimal.Decimal
	var issuance decimal.NullDecimal
	for rows.Next() {
		block := &execdb.Block{}
		err := rows.Scan(
			&block.Height,
			&block.Hash,
			&block.BaseFee,
			&block.Difficulty,
			&block.ExtraData,
			&block.GasLimit,
			&block.GasUsed,
			&block.FeeRecipient,
			&block.ParentHash,
			&block.Size,
			&block.StateRoot,
			&block.Timestamp,
			&totalDifficulty,
			&issuance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		block.TotalDifficulty = totalDifficulty.BigInt()
		if issuance.Valid {
			block.Issuance = issuance.Decimal.BigInt()
		}
		blocks = append(blocks, block)
	}

	// Always return in order of height.
	sort.Slice(blocks, func(i int, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})
	return blocks, nil
}
