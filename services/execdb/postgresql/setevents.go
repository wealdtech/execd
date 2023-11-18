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

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
)

// SetEvents sets multiple events efficiently.
func (s *Service) SetEvents(ctx context.Context, events []*execdb.Event) error {
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
		pgx.Identifier{"t_events"},
		[]string{
			"f_transaction_hash",
			"f_block_height",
			"f_index",
			"f_address",
			"f_topics",
			"f_data",
		},
		pgx.CopyFromSlice(len(events), func(i int) ([]interface{}, error) {
			return []interface{}{
				events[i].TransactionHash,
				events[i].BlockHeight,
				events[i].Index,
				events[i].Address,
				events[i].Topics,
				events[i].Data,
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

		log.Debug().Err(err).Msg("Failed to copy insert events; applying one at a time")
		for _, event := range events {
			if err := s.SetEvent(ctx, event); err != nil {
				return err
			}
		}
	}

	return nil
}
