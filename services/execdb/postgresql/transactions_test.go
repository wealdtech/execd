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
	"encoding/json"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/execdb/postgresql"
)

func uint32Address(input uint32) *uint32 {
	return &input
}

func TestTransactions(t *testing.T) {
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
		name         string
		filter       *execdb.TransactionFilter
		transactions string
		err          string
	}{
		{
			name: "SingleBlockFirstTransaction",
			filter: &execdb.TransactionFilter{
				Limit: 1,
				Order: execdb.OrderEarliest,
				From:  uint32Address(11904612),
				To:    uint32Address(11904612),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","Index":0,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":188681,"GasPrice":273000000000,"GasUsed":130654,"Hash":"KgcDeE6jzoS85WQtf/+/4ZErMxy3/rHGzIgMUpHK7Wk=","Input":"GMuv5QAAAAAAAAAAAAAAAAAAAAAAAAAAAAACHhngybqyQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACXEYcdOCn4QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAAAAAAAHGD36N6PgAEXhmkyZ+mO5HiBAKJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGAzM2oAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAFG0snp70pb9NMp8Rp9J1bzX/lE3AAAAAAAAAAAAAAAAwCqqObIj/o0KDlxPJ+rZCDx1bMI=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":8,"R":77319052415362366005528604658861065148636062326643923511745025424565677890390,"S":33686791537244881214546531251837409130332235643772944342572604547370481920068,"Status":1,"To":"eiUNVjC0z1OXOd8sXay0xlnySI0=","V":38,"Value":0}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.Transactions(ctx, test.filter)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				transactions, err := json.Marshal(res)
				require.NoError(t, err)
				require.Equal(t, test.transactions, string(transactions))
			}
		})
	}
}
