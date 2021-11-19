// Copyright © 2021 Weald Technology Limited.
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

package postgresql_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/execdb/postgresql"
)

func TestSetBlock(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithServer(os.Getenv("EXECDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("EXECDB_PORT"))),
		postgresql.WithUser(os.Getenv("EXECDB_USER")),
		postgresql.WithPassword(os.Getenv("EXECDB_PASSWORD")),
	)
	require.NoError(t, err)

	tests := []struct {
		name  string
		block *execdb.Block
		err   string
	}{
		{
			name: "Nil",
			err:  "block nil",
		},
		{
			name: "Good",
			block: &execdb.Block{
				Height:          1,
				Hash:            byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				BaseFee:         2,
				Difficulty:      123456,
				ExtraData:       byteArray("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
				GasLimit:        30000000,
				GasUsed:         10000000,
				FeeRecipient:    byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
				ParentHash:      byteArray("0x0000000000000000000000000000000000000000000000000000000000000000"),
				Size:            2000000,
				StateRoot:       byteArray("0x02030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021"),
				Timestamp:       time.Now(),
				TotalDifficulty: bigInt("11123456"),
			},
		},
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.SetBlock(ctx, test.block)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
