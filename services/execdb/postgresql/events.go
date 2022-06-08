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
	"github.com/wealdtech/execd/services/execdb"
)

// Events returns events matching the supplied filter.
func (s *Service) Events(ctx context.Context, filter *execdb.EventFilter) ([]*execdb.Event, error) {
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
      ,f_index
      ,f_address
      ,f_topics
      ,f_data
FROM t_events`)

	wherestr := "WHERE"

	if filter.Address != nil {
		queryVals = append(queryVals, *filter.Address)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_address = $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.TransactionHash != nil {
		queryVals = append(queryVals, *filter.TransactionHash)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_transaction_hash = $%d`, wherestr, len(queryVals)))
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

	if filter.Limit != nil {
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

	events := make([]*execdb.Event, 0)
	for rows.Next() {
		event := &execdb.Event{}
		err := rows.Scan(
			&event.TransactionHash,
			&event.BlockHeight,
			&event.Index,
			&event.Address,
			&event.Topics,
			&event.Data,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		events = append(events, event)
	}

	// Always return order of block then index.
	sort.Slice(events, func(i int, j int) bool {
		if events[i].BlockHeight != events[j].BlockHeight {
			return events[i].BlockHeight < events[j].BlockHeight
		}
		return events[i].Index < events[j].Index
	})
	return events, nil
}
