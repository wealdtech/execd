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
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

type schemaMetadata struct {
	Version uint64 `json:"version"`
}

var currentVersion = uint64(5)

type upgrade struct {
	funcs []func(context.Context, *Service) error
}

var upgrades = map[uint64]*upgrade{
	2: {
		funcs: []func(context.Context, *Service) error{
			addBlockMEV,
			addTransactionAccessLists,
		},
	},
	3: {
		funcs: []func(context.Context, *Service) error{
			addForeignKeys,
		},
	},
	4: {
		funcs: []func(context.Context, *Service) error{
			fixGasPrice,
		},
	},
	5: {
		funcs: []func(context.Context, *Service) error{
			addBalancesTable,
		},
	},
}

// Upgrade upgrades the database.
func (s *Service) Upgrade(ctx context.Context) error {
	// See if we have anything at all.
	tableExists, err := s.tableExists(ctx, "t_metadata")
	if err != nil {
		return errors.Wrap(err, "failed to check presence of tables")
	}
	if !tableExists {
		return s.Init(ctx)
	}

	version, err := s.version(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain version")
	}

	log.Trace().Uint64("current_version", version).Uint64("required_version", currentVersion).Msg("Checking if database upgrade is required")
	if version == currentVersion {
		// Nothing to do.
		return nil
	}

	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin upgrade transaction")
	}

	for i := version + 1; i <= currentVersion; i++ {
		log.Info().Uint64("target_version", i).Msg("Upgrading database")
		if upgrade, exists := upgrades[i]; exists {
			for i, upgradeFunc := range upgrade.funcs {
				log.Info().Int("current", i+1).Int("total", len(upgrade.funcs)).Msg("Running upgrade function")
				if err := upgradeFunc(ctx, s); err != nil {
					cancel()
					return errors.Wrap(err, "failed to upgrade")
				}
			}
		}
	}

	if err := s.setVersion(ctx, currentVersion); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set latest schema version")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit upgrade transaction")
	}

	log.Info().Msg("Upgrade complete")

	return nil
}

// // columnExists returns true if the given column exists in the given table.
// func (s *Service) columnExists(ctx context.Context, tableName string, columnName string) (bool, error) {
// 	tx := s.tx(ctx)
// 	if tx == nil {
// 		ctx, cancel, err := s.BeginTx(ctx)
// 		if err != nil {
// 			return false, errors.Wrap(err, "failed to begin transaction")
// 		}
// 		tx = s.tx(ctx)
// 		defer cancel()
// 	}
//
// 	query := fmt.Sprintf(`SELECT true
// FROM pg_attribute
// WHERE attrelid = '%s'::regclass
//   AND attname = '%s'
//   AND NOT attisdropped`, tableName, columnName)
//
// 	rows, err := tx.Query(ctx, query)
// 	if err != nil {
// 		return false, err
// 	}
// 	defer rows.Close()
//
// 	found := false
// 	if rows.Next() {
// 		err = rows.Scan(
// 			&found,
// 		)
// 		if err != nil {
// 			return false, errors.Wrap(err, "failed to scan row")
// 		}
// 	}
// 	return found, nil
// }

// tableExists returns true if the given table exists.
func (s *Service) tableExists(ctx context.Context, tableName string) (bool, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	rows, err := tx.Query(ctx, `SELECT true
FROM information_schema.tables
WHERE table_schema = (SELECT current_schema())
  AND table_name = $1`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	found := false
	if rows.Next() {
		err = rows.Scan(
			&found,
		)
		if err != nil {
			return false, errors.Wrap(err, "failed to scan row")
		}
	}
	return found, nil
}

// version obtains the version of the schema.
func (s *Service) version(ctx context.Context) (uint64, error) {
	data, err := s.Metadata(ctx, "schema")
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain schema metadata")
	}

	// No data means it's version 0 of the schema.
	if len(data) == 0 {
		return 0, nil
	}

	var metadata schemaMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal metadata JSON")
	}

	return metadata.Version, nil
}

// setVersion sets the version of the schema.
func (s *Service) setVersion(ctx context.Context, version uint64) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	metadata := &schemaMetadata{
		Version: version,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}

	return s.SetMetadata(ctx, "schema", data)
}

// Init initialises the database.
func (s *Service) Init(ctx context.Context) error {
	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin initial tables transaction")
	}
	tx := s.tx(ctx)
	if tx == nil {
		cancel()
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
-- t_metadata stores data about execd processing functions.
CREATE TABLE t_metadata (
  f_key    TEXT NOT NULL PRIMARY KEY
 ,f_value JSONB NOT NULL
);
CREATE UNIQUE INDEX i_metadata_1 ON t_metadata(f_key);
INSERT INTO t_metadata VALUES('schema', '{"version": 5}');

-- t_blocks contains execution layer blocks.
CREATE TABLE t_blocks (
  f_height           INTEGER NOT NULL
 ,f_hash             BYTEA NOT NULL
 ,f_base_fee         BIGINT
 ,f_difficulty       BIGINT NOT NULL
 ,f_extra_data       BYTEA NOT NULL
 ,f_gas_limit        INTEGER NOT NULL
 ,f_gas_used         INTEGER NOT NULL
 ,f_fee_recipient    BYTEA NOT NULL
 ,f_parent_hash      BYTEA NOT NULL -- cannot enforce contraint here as parent of block 0 is 0
 ,f_size             INTEGER NOT NULL
 ,f_state_root       BYTEA NOT NULL
 ,f_timestamp        TIMESTAMPTZ NOT NULL
 ,f_total_difficulty NUMERIC NOT NULL
 ,f_issuance         NUMERIC
);
CREATE UNIQUE INDEX i_blocks_1 ON t_blocks(f_height,f_hash);
CREATE UNIQUE INDEX i_blocks_2 ON t_blocks(f_hash);

-- t_transasctions contains execution layer transactions.
CREATE TABLE t_transactions (
  f_block_height             INTEGER NOT NULL
 ,f_block_hash               BYTEA NOT NULL REFERENCES t_blocks(f_hash) ON DELETE CASCADE
 ,f_contract_address         BYTEA
 ,f_index                    INTEGER NOT NULL
 ,f_type                     BIGINT NOT NULL
 ,f_from                     BYTEA NOT NULL
 ,f_gas_limit                INTEGER NOT NULL
 ,f_gas_price                BIGINT NOT NULL
 ,f_gas_used                 INTEGER NOT NULL
 ,f_hash                     BYTEA NOT NULL
 ,f_input                    BYTEA
 ,f_max_fee_per_gas          BIGINT
 ,f_max_priority_fee_per_gas BIGINT
 ,f_nonce                    BIGINT NOT NULL
 ,f_r                        BYTEA NOT NULL
 ,f_s                        BYTEA NOT NULL
 ,f_status                   INTEGER NOT NULL
 ,f_to                       BYTEA
 ,f_v                        BYTEA NOT NULL
 ,f_value                    NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_transactions_1 ON t_transactions(f_block_hash,f_index);
CREATE INDEX i_transactions_2 ON t_transactions(f_from,f_block_height);
CREATE INDEX i_transactions_3 ON t_transactions(f_to,f_block_height);
CREATE INDEX i_transactions_4 ON t_transactions(f_block_height);
CREATE UNIQUE INDEX i_transactions_5 ON t_transactions(f_hash);

CREATE TABLE t_transaction_access_lists (
  f_transaction_hash BYTEA NOT NULL REFERENCES t_transactions(f_hash) ON DELETE CASCADE
 ,f_block_height     INTEGER NOT NULL
 ,f_address          BYTEA NOT NULL
 ,f_storage_keys     BYTEA[] NOT NULL
);
CREATE UNIQUE INDEX i_transaction_access_lists_1 ON t_transaction_access_lists(f_transaction_hash,f_block_height,f_address);
CREATE INDEX i_transaction_access_lists_2 ON t_transaction_access_lists(f_address);
CREATE INDEX i_transaction_access_lists_3 ON t_transaction_access_lists(f_block_height);

-- t_transaction_balance_changes contains balance changes as a result of a transaction.
CREATE TABLE t_transaction_balance_changes (
  f_transaction_hash BYTEA NOT NULL REFERENCES t_transactions(f_hash) ON DELETE CASCADE
 ,f_block_height     INTEGER NOT NULL
 ,f_address          BYTEA NOT NULL
 ,f_old              NUMERIC NOT NULL
 ,f_new              NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_transaction_balance_changes_1 ON t_transaction_balance_changes(f_transaction_hash,f_block_height,f_address);
CREATE INDEX i_transaction_balance_changes_2 ON t_transaction_balance_changes(f_address);
CREATE INDEX i_transaction_balance_changes_3 ON t_transaction_balance_changes(f_block_height);

-- t_transaction_storage_changes contains storage changes as a result of a transaction.
CREATE TABLE t_transaction_storage_changes (
  f_transaction_hash BYTEA NOT NULL
 ,f_block_height     INTEGER NOT NULL
 ,f_address          BYTEA NOT NULL
 ,f_storage_address  BYTEA NOT NULL
 ,f_value            BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_transaction_storage_changes_1 ON t_transaction_storage_changes(f_transaction_hash,f_address,f_storage_address);
CREATE INDEX i_transaction_storage_changes_2 ON t_transaction_storage_changes(f_address);
CREATE INDEX i_transaction_storage_changes_3 ON t_transaction_storage_changes(f_block_height);

-- t_events contains execution layer events.
CREATE TABLE t_events (
  f_transaction_hash BYTEA NOT NULL REFERENCES t_transactions(f_hash) ON DELETE CASCADE
 ,f_block_height     INTEGER NOT NULL
 ,f_index            INTEGER NOT NULL
 ,f_address          BYTEA NOT NULL
 ,f_topics           BYTEA[] NOT NULL
 ,f_data             BYTEA
);
CREATE UNIQUE INDEX i_events_1 ON t_events(f_transaction_hash,f_block_height,f_index);
CREATE INDEX i_events_2 ON t_events(f_address);
CREATE INDEX i_events_3 ON t_events(f_block_height);

-- t_block_mevs contains block MEV.
CREATE TABLE t_block_mevs (
  f_block_hash   BYTEA NOT NULL REFERENCES t_blocks(f_hash) ON DELETE CASCADE
 ,f_block_height INTEGER NOT NULL
 ,f_fees         NUMERIC NOT NULL
 ,f_payments     NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_block_mevs_1 ON t_block_mevs(f_block_hash);
CREATE INDEX i_block_mevs_2 ON t_block_mevs(f_block_height);

-- t_balances contains balances on addresses.
CREATE TABLE t_balances (
  f_address  BYTEA NOT NULL
 ,f_currency BYTEA NOT NULL
 ,f_from     TIMESTAMPTZ NOT NULL
 ,f_amount   NUMERIC NOT NULL
);
CREATE UNIQUE INDEX i_balances_1 ON t_balances(f_address,f_currency,f_from);

`); err != nil {
		cancel()
		return errors.Wrap(err, "failed to create initial tables")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit initial tables transaction")
	}

	return nil
}

// addBlockMEV creates the t_block_mevs table.
func addBlockMEV(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	tableExists, err := s.tableExists(ctx, "t_block_mevs")
	if err != nil {
		return errors.Wrap(err, "failed to check presence of t_block_mevs")
	}
	if tableExists {
		return nil
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_block_mevs (
  f_block_hash   BYTEA NOT NULL REFERENCES t_blocks(f_hash) ON DELETE CASCADE
 ,f_block_height INTEGER NOT NULL
 ,f_fees         NUMERIC NOT NULL
 ,f_payments     NUMERIC NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create t_block_mevs")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_block_mevs_1 ON t_block_mevs(f_block_hash)
`); err != nil {
		return errors.Wrap(err, "failed to create i_block_mevs_1")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX i_block_mevs_2 ON t_block_mevs(f_block_height);
`); err != nil {
		return errors.Wrap(err, "failed to create i_block_mevs_2")
	}

	return nil
}

// addTransactionAccessLists creates the t_transaction_access_lists table.
func addTransactionAccessLists(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	tableExists, err := s.tableExists(ctx, "t_transaction_access_lists")
	if err != nil {
		return errors.Wrap(err, "failed to check presence of t_transaction_access_lists")
	}
	if tableExists {
		return nil
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_transaction_access_lists (
  f_transaction_hash BYTEA NOT NULL
 ,f_block_height     INTEGER NOT NULL
 ,f_address          BYTEA NOT NULL
 ,f_storage_keys     BYTEA[] NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create t_transaction_access_lists")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_transaction_access_lists_1 ON t_transaction_access_lists(f_transaction_hash,f_block_height,f_address);
`); err != nil {
		return errors.Wrap(err, "failed to create i_transaction_access_lists_1")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX i_transaction_access_lists_2 ON t_transaction_access_lists(f_address);
`); err != nil {
		return errors.Wrap(err, "failed to create i_transaction_access_lists_2")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX i_transaction_access_lists_3 ON t_transaction_access_lists(f_block_height);
`); err != nil {
		return errors.Wrap(err, "failed to create i_transaction_access_lists_3")
	}
	return nil
}

// addForeignKeys adds foreign keys to relevant tables.
func addForeignKeys(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// See if we need to fix the transactions index.
	var goodIndices uint64
	err := tx.QueryRow(ctx, `
SELECT COUNT(*)
FROM pg_indexes
WHERE indexname = 'i_transactions_5'
  AND indexdef LIKE '%UNIQUE%'
`).Scan(
		&goodIndices,
	)
	if err != nil {
		return errors.Wrap(err, "failed to obtain transactions index  information")
	}
	log.Trace().Uint64("good_indices", goodIndices).Msg("Found number of good indices")
	if goodIndices == 0 {
		// Need to fix the index.
		_, err := tx.Exec(ctx, `
DROP INDEX IF EXISTS i_transactions_5`)
		if err != nil {
			return errors.Wrap(err, "failed to drop i_transactions_5")
		}

		_, err = tx.Exec(ctx, `
CREATE UNIQUE INDEX i_transactions_5 ON t_transactions(f_hash)`)
		if err != nil {
			return errors.Wrap(err, "failed to create i_transactions_5")
		}
	}

	_, err = tx.Exec(ctx, `
ALTER TABLE t_events
ADD CONSTRAINT t_events_f_transaction_hash_fkey
  FOREIGN KEY (f_transaction_hash)
  REFERENCES t_transactions(f_hash)
  ON DELETE CASCADE`)
	if err != nil {
		return errors.Wrap(err, "failed to set foreign key for t_events")
	}

	_, err = tx.Exec(ctx, `
ALTER TABLE t_transaction_access_lists
ADD CONSTRAINT t_transaction_access_lists_f_transaction_hash_fkey
  FOREIGN KEY (f_transaction_hash)
  REFERENCES t_transactions(f_hash)
  ON DELETE CASCADE`)
	if err != nil {
		return errors.Wrap(err, "failed to set foreign key for t_transaction_access_lists")
	}

	_, err = tx.Exec(ctx, `
ALTER TABLE t_transaction_balance_changes
ADD CONSTRAINT t_transaction_balance_changes_f_transaction_hash_fkey
  FOREIGN KEY (f_transaction_hash)
  REFERENCES t_transactions(f_hash)
  ON DELETE CASCADE`)
	if err != nil {
		return errors.Wrap(err, "failed to set foreign key for t_transaction_balance_changes")
	}

	_, err = tx.Exec(ctx, `
ALTER TABLE t_transaction_storage_changes
ADD CONSTRAINT t_transaction_storage_changes_f_transaction_hash_fkey
  FOREIGN KEY (f_transaction_hash)
  REFERENCES t_transactions(f_hash)
  ON DELETE CASCADE`)
	if err != nil {
		return errors.Wrap(err, "failed to set foreign key for t_transaction_storage_changes")
	}

	return nil
}

// fixGasPrice fixes the f_gas_price column in the t_transactions table.
func fixGasPrice(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Fetch the minimum height.
	minHeight := uint64(0)
	err := tx.QueryRow(ctx, `
SELECT COALESCE(MIN(f_block_height),999999999)
FROM t_transactions
WHERE f_type = 2
  AND f_gas_price IS NULL`,
	).Scan(
		&minHeight,
	)
	if err != nil {
		return errors.Wrap(err, "failed to obtain minimum block height")
	}

	// Fetch the maximum height.
	maxHeight := uint64(0)
	err = tx.QueryRow(ctx, `
SELECT COALESCE(MAX(f_height),0)
FROM t_blocks`,
	).Scan(
		&maxHeight,
	)
	if err != nil {
		return errors.Wrap(err, "failed to obtain maximum block height")
	}

	for height := minHeight; height <= maxHeight; height++ {
		log.Trace().Uint64("block_height", height).Msg("Fixing gas price for transactions in block")
		// Obtain the base fee for the block.
		baseFee := uint64(0)
		if err := tx.QueryRow(ctx, `
SELECT f_base_fee
FROM t_blocks
WHERE f_height = $1`,
			height,
		).Scan(
			&baseFee,
		); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to obtain base fee for block %d", height))
		}

		// Update the gas price for the type 2 transactions in the block.
		_, err := tx.Exec(ctx, `
UPDATE t_transactions
SET f_gas_price = LEAST(f_max_priority_fee_per_gas, f_max_fee_per_gas - $1) + $1
WHERE f_block_height = $2
  AND f_type = 2
  AND f_gas_price IS NULL`,
			baseFee,
			height,
		)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to obtain base fee for block %d", height))
		}
	}

	// Add the 'not null' constraint to the gas price column.
	_, err = tx.Exec(ctx, `
ALTER TABLE t_transactions
ALTER COLUMN f_gas_price
SET NOT NULL`)
	if err != nil {
		return errors.Wrap(err, "failed to set f_gas_price to not null")
	}

	return nil
}

// addBalancesTable adds the t_balances table.
func addBalancesTable(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	tableExists, err := s.tableExists(ctx, "t_balances")
	if err != nil {
		return errors.Wrap(err, "failed to check presence of t_balances")
	}
	if tableExists {
		return nil
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_balances (
  f_address  BYTEA NOT NULL
 ,f_currency BYTEA NOT NULL
 ,f_from     TIMESTAMPTZ NOT NULL
 ,f_amount   NUMERIC NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create t_balances")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_balances_1 ON t_balances(f_address,f_currency,f_from);
`); err != nil {
		return errors.Wrap(err, "failed to create i_balances_1")
	}

	return nil
}
