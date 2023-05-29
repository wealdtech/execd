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
	"strings"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/execd/services/execdb"
)

// TransactionBalanceChanges returns transaction balance changes matching the supplied filter.
func (s *Service) TransactionBalanceChanges(ctx context.Context, filter *execdb.TransactionBalanceChangeFilter) ([]*execdb.TransactionBalanceChange, error) {
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
SELECT f_transaction_hash
      ,f_block_height
      ,f_address
      ,f_old
      ,f_new
FROM t_transaction_balance_changes
`)

	wherestr := "WHERE"

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
		wherestr = "  AND"
	}

	if len(filter.TxHashes) != 0 {
		queryVals = append(queryVals, filter.TxHashes)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_transaction_hash = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.Addresses) != 0 {
		queryVals = append(queryVals, filter.Addresses)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_address = ANY($%d)`, wherestr, len(queryVals)))
		// wherestr = "  AND"
	}

	switch filter.Order {
	case execdb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_block_height`)
	case execdb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_block_height DESC`)
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

	transactionBalanceChanges := make([]*execdb.TransactionBalanceChange, 0)
	var oldValue decimal.Decimal
	var newValue decimal.Decimal
	for rows.Next() {
		transactionBalanceChange := &execdb.TransactionBalanceChange{}
		err := rows.Scan(
			&transactionBalanceChange.TransactionHash,
			&transactionBalanceChange.BlockHeight,
			&transactionBalanceChange.Address,
			&oldValue,
			&newValue,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		transactionBalanceChange.Old = oldValue.BigInt()
		transactionBalanceChange.New = newValue.BigInt()
		transactionBalanceChanges = append(transactionBalanceChanges, transactionBalanceChange)
	}

	return transactionBalanceChanges, nil
}
