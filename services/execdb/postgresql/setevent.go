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
	"github.com/wealdtech/execd/services/execdb"
)

// SetEvent sets an event.
func (s *Service) SetEvent(ctx context.Context, event *execdb.Event) error {
	if event == nil {
		return errors.New("event nil")
	}

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_events(f_transaction_hash
                    ,f_block_height
                    ,f_index
                    ,f_address
                    ,f_topics
                    ,f_data
                    )
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (f_transaction_hash,f_block_height,f_index) DO
UPDATE
SET f_address = excluded.f_address
   ,f_topics = excluded.f_topics
   ,f_data = excluded.f_data
`,
		event.TransactionHash,
		event.BlockHeight,
		event.Index,
		event.Address,
		event.Topics,
		event.Data,
	)

	return err
}
