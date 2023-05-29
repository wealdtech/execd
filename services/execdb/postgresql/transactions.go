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
	"math/big"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// Transactions returns transactions matching the supplied filter.
func (s *Service) Transactions(ctx context.Context, filter *execdb.TransactionFilter) ([]*execdb.Transaction, error) {
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
      ,f_index
      ,f_type
      ,f_from
      ,f_gas_limit
      ,f_gas_price
      ,f_gas_used
      ,f_hash
      ,f_input
      ,f_max_fee_per_gas
      ,f_max_priority_fee_per_gas
      ,f_nonce
      ,f_r
      ,f_s
      ,f_status
      ,f_to
      ,f_v
      ,f_value
FROM t_transactions`)

	wherestr := "WHERE"

	if filter.Sender != nil {
		queryVals = append(queryVals, *filter.Sender)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_from = $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.Recipient != nil {
		queryVals = append(queryVals, *filter.Recipient)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_to = $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_block_height >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_block_height <= $%d`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case execdb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_block_height,f_index`)
	case execdb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_block_height DESC,f_index DESC`)
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

	transactions := make([]*execdb.Transaction, 0)
	for rows.Next() {
		transaction := &execdb.Transaction{}
		r := make([]byte, 0)
		s := make([]byte, 0)
		v := make([]byte, 0)
		value := decimal.Zero
		err := rows.Scan(
			&transaction.BlockHeight,
			&transaction.BlockHash,
			&transaction.Index,
			&transaction.Type,
			&transaction.From,
			&transaction.GasLimit,
			&transaction.GasPrice,
			&transaction.GasUsed,
			&transaction.Hash,
			&transaction.Input,
			&transaction.MaxFeePerGas,
			&transaction.MaxPriorityFeePerGas,
			&transaction.Nonce,
			&r,
			&s,
			&transaction.Status,
			&transaction.To,
			&v,
			&value,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		transaction.R = new(big.Int).SetBytes(r)
		transaction.S = new(big.Int).SetBytes(s)
		transaction.V = new(big.Int).SetBytes(v)
		var success bool
		transaction.Value, success = new(big.Int).SetString(value.String(), 10)
		if !success {
			return nil, errors.New("Failed to obtain value")
		}
		transactions = append(transactions, transaction)
	}

	// Always return order of block then index.
	sort.Slice(transactions, func(i int, j int) bool {
		if transactions[i].BlockHeight != transactions[j].BlockHeight {
			return transactions[i].BlockHeight < transactions[j].BlockHeight
		}
		return transactions[i].Index < transactions[j].Index
	})
	return transactions, nil
}
