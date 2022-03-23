// Copyright © 2022 Weald Technology Limited.
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
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/execdb/postgresql"
)

func TestSetBalance(t *testing.T) {
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
		name    string
		balance *execdb.Balance
		err     string
	}{
		{
			name: "Nil",
			err:  "balance nil",
		},
		{
			name: "Good",
			balance: &execdb.Balance{
				Address:  address("0x000102030405060708090a0b0c0d0e0f10111213"),
				Currency: "WEI",
				From:     time.Unix(1600000000, 0),
				Amount:   decimal.NewFromInt(1234567890),
			},
		},
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.SetBalance(ctx, test.balance)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
