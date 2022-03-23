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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
)

// Balances returns balances matching the supplied filter.
func (s *Service) Balances(ctx context.Context, filter *execdb.BalanceFilter) ([]*execdb.Balance, error) {
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
SELECT f_address
      ,f_currency
      ,f_from
      ,f_amount
FROM t_balances`)

	wherestr := "WHERE"

	if len(filter.Holders) > 0 {
		queryVals = append(queryVals, filter.Holders)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_address = ANY($%d)`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.Currency != "" {
		queryVals = append(queryVals, filter.Currency)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_currency = $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_from >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_from <= $%d`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case execdb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_from`)
	case execdb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_from DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit > 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	balances := make([]*execdb.Balance, 0)
	for rows.Next() {
		balance := &execdb.Balance{}
		address := make([]byte, 20)
		err := rows.Scan(
			&address,
			&balance.Currency,
			&balance.From,
			&balance.Amount,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(balance.Address[:], address)
		balances = append(balances, balance)
	}

	// Always return in order of holder/currency/timstamp.
	sort.Slice(balances, func(i int, j int) bool {
		order := bytes.Compare(balances[i].Address[:], balances[j].Address[:])
		if order != 0 {
			return order < 0
		}
		order = strings.Compare(balances[i].Currency, balances[j].Currency)
		if order != 0 {
			return order < 0
		}
		return balances[i].From.Unix() < balances[j].From.Unix()
	})
	return balances, nil
}
