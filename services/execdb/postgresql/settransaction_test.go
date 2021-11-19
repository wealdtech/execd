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

func TestSetTransaction(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithServer(os.Getenv("EXECDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("EXECDB_PORT"))),
		postgresql.WithUser(os.Getenv("EXECDB_USER")),
		postgresql.WithPassword(os.Getenv("EXECDB_PASSWORD")),
	)
	require.NoError(t, err)

	gasPrice := uint64(100000000000)
	maxFeePerGas := uint64(120000000000)
	maxPriorityFeePerGas := uint64(2000000000)
	tests := []struct {
		name        string
		transaction *execdb.Transaction
		err         string
	}{
		{
			name: "Nil",
			err:  "transaction nil",
		},
		{
			name: "Good",
			transaction: &execdb.Transaction{
				BlockHeight:          123,
				BlockHash:            byteArray("0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				Index:                1,
				Type:                 2,
				From:                 byteArray("0x000102030405060708090a0b0c0d0e0f10111213"),
				GasLimit:             100000,
				GasPrice:             &gasPrice,
				GasUsed:              21000,
				Hash:                 byteArray("0x100102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
				Input:                []byte{},
				MaxFeePerGas:         &maxFeePerGas,
				MaxPriorityFeePerGas: &maxPriorityFeePerGas,
				Nonce:                1,
				R:                    bigInt("1"),
				S:                    bigInt("2"),
				Status:               1,
				To:                   byteArray("0x0102030405060708090a0b0c0d0e0f1011121314"),
				V:                    bigInt("3"),
				Value:                bigInt("12345000000000000000000"),
			},
		},
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	// Create a dummy block against which to work.
	require.NoError(t, s.SetBlock(ctx, &execdb.Block{
		Height:          123,
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
	}))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.SetTransaction(ctx, test.transaction)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.Nil(t, err)
			}
		})
	}
}
