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
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/attestantio/go-execution-client/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/execdb/postgresql"
)

func uint32Ptr(input uint32) *uint32 {
	return &input
}

func addressPtr(input string) *[]byte {
	res, err := hex.DecodeString(strings.TrimPrefix(input, "0x"))
	if err != nil {
		panic(err)
	}
	return &res
}

func address(input string) types.Address {
	tmp, err := hex.DecodeString(strings.TrimPrefix(input, "0x"))
	if err != nil {
		panic(err)
	}

	res := types.Address{}
	copy(res[:], tmp)
	return res
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
				Limit: uint32Ptr(1),
				Order: execdb.OrderEarliest,
				From:  uint32Ptr(11904612),
				To:    uint32Ptr(11904612),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","ContractAddress":null,"Index":0,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":188681,"GasPrice":273000000000,"GasUsed":130654,"Hash":"KgcDeE6jzoS85WQtf/+/4ZErMxy3/rHGzIgMUpHK7Wk=","Input":"GMuv5QAAAAAAAAAAAAAAAAAAAAAAAAAAAAACHhngybqyQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACXEYcdOCn4QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAAAAAAAHGD36N6PgAEXhmkyZ+mO5HiBAKJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGAzM2oAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAFG0snp70pb9NMp8Rp9J1bzX/lE3AAAAAAAAAAAAAAAAwCqqObIj/o0KDlxPJ+rZCDx1bMI=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":8,"R":77319052415362366005528604658861065148636062326643923511745025424565677890390,"S":33686791537244881214546531251837409130332235643772944342572604547370481920068,"Status":1,"To":"eiUNVjC0z1OXOd8sXay0xlnySI0=","V":38,"Value":0}]`,
		},
		{
			name: "SingleBlockLastTransaction",
			filter: &execdb.TransactionFilter{
				Limit: uint32Ptr(1),
				Order: execdb.OrderLatest,
				From:  uint32Ptr(11904612),
				To:    uint32Ptr(11904612),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","ContractAddress":null,"Index":200,"Type":0,"From":"PuKNVOssStgklwLm7MtF8m1yiQ4=","GasLimit":41209,"GasPrice":118000000000,"GasUsed":41209,"Hash":"i/UC+Vublx1FVNs6pa4X6wNcp7FP23vvajOMEXfIq6Y=","Input":"qQWcuwAAAAAAAAAAAAAAAKtCyCtTvUXfOhE0+zG156I/O0b+AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADT3uRA=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":7161,"R":29531117702410677937339437069303595544556097890063779964323882043416083150353,"S":4161996985285313327249024077702407997857952680692429012556181576023728048920,"Status":1,"To":"2sF/lY0u5SOiIGIGmUWXwT2DHsc=","V":28,"Value":0}]`,
		},
		{
			name: "SingleBlockLastTwoTransactions",
			filter: &execdb.TransactionFilter{
				Limit: uint32Ptr(2),
				Order: execdb.OrderLatest,
				From:  uint32Ptr(11904612),
				To:    uint32Ptr(11904612),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","ContractAddress":null,"Index":199,"Type":0,"From":"AAC/f05Lf7IxX8bV0PiFTJHf8dg=","GasLimit":35000,"GasPrice":121000000000,"GasUsed":21000,"Hash":"uGIstGM5fnda2nVd0scIMYBdd+K+QSzFOlav26ZqHDo=","Input":null,"MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":36763,"R":60627120632572549679933702040237846415272723338957899682800251277821128215779,"S":53254448744185249236514054073938120165288019972019549060701028967779472165716,"Status":1,"To":"ANBto/R25Wp/TsfNbXL1Tfaxb08=","V":38,"Value":20000000000000000},{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","ContractAddress":null,"Index":200,"Type":0,"From":"PuKNVOssStgklwLm7MtF8m1yiQ4=","GasLimit":41209,"GasPrice":118000000000,"GasUsed":41209,"Hash":"i/UC+Vublx1FVNs6pa4X6wNcp7FP23vvajOMEXfIq6Y=","Input":"qQWcuwAAAAAAAAAAAAAAAKtCyCtTvUXfOhE0+zG156I/O0b+AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADT3uRA=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":7161,"R":29531117702410677937339437069303595544556097890063779964323882043416083150353,"S":4161996985285313327249024077702407997857952680692429012556181576023728048920,"Status":1,"To":"2sF/lY0u5SOiIGIGmUWXwT2DHsc=","V":28,"Value":0}]`,
		},
		{
			name: "SenderTwoTransactions",
			filter: &execdb.TransactionFilter{
				Limit:  uint32Ptr(2),
				Order:  execdb.OrderEarliest,
				From:   uint32Ptr(11904612),
				Sender: addressPtr("0x7183dfa37a3e00045e19a4c99fa63b91e2040289"),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904612,"BlockHash":"bk8D+S99bK2CIrlvMHndkjjuDJvZWA0QrZKoXRqumuQ=","ContractAddress":null,"Index":0,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":188681,"GasPrice":273000000000,"GasUsed":130654,"Hash":"KgcDeE6jzoS85WQtf/+/4ZErMxy3/rHGzIgMUpHK7Wk=","Input":"GMuv5QAAAAAAAAAAAAAAAAAAAAAAAAAAAAACHhngybqyQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACXEYcdOCn4QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAAAAAAAHGD36N6PgAEXhmkyZ+mO5HiBAKJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGAzM2oAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAFG0snp70pb9NMp8Rp9J1bzX/lE3AAAAAAAAAAAAAAAAwCqqObIj/o0KDlxPJ+rZCDx1bMI=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":8,"R":77319052415362366005528604658861065148636062326643923511745025424565677890390,"S":33686791537244881214546531251837409130332235643772944342572604547370481920068,"Status":1,"To":"eiUNVjC0z1OXOd8sXay0xlnySI0=","V":38,"Value":0},{"AccessList":null,"BlockHeight":11904624,"BlockHash":"pEYcgdeHxmwfnY9Qg6vTyuVvy29nqc39YrjZoWAWGH0=","ContractAddress":null,"Index":3,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":188696,"GasPrice":279000000000,"GasUsed":115666,"Hash":"1aDT+JhwG1FNQeG7JGEeB1HlrGdCVYLOHvSR4BdHcj0=","Input":"GMuv5QAAAAAAAAAAAAAAAAAAAAAAAAAAAAACnNs+sKQr4EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACxYwFaZk61AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAAAAAAAHGD36N6PgAEXhmkyZ+mO5HiBAKJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGAzNJQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAFG0snp70pb9NMp8Rp9J1bzX/lE3AAAAAAAAAAAAAAAAwCqqObIj/o0KDlxPJ+rZCDx1bMI=","MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":9,"R":91467469473910231354137087645144238786306277121923845247827531477773776853665,"S":28833033981376353107155330734640823950762500846879455602621666575455679279649,"Status":1,"To":"eiUNVjC0z1OXOd8sXay0xlnySI0=","V":37,"Value":0}]`,
		},
		{
			name: "SenderTwoLatestTransactions",
			filter: &execdb.TransactionFilter{
				Limit:  uint32Ptr(2),
				Order:  execdb.OrderLatest,
				From:   uint32Ptr(11904611),
				Sender: addressPtr("0x7183dfa37a3e00045e19a4c99fa63b91e2040289"),
			},
			transactions: `[{"AccessList":null,"BlockHeight":11904806,"BlockHash":"2jcz7zj8VUVuKiF5btTTdUMRS/mn9eDtHmYkl/PntMI=","ContractAddress":null,"Index":124,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":21000,"GasPrice":140000000000,"GasUsed":21000,"Hash":"Ib5NG+MO1zrNxur0EPV7dzVPq6RStDqxKKnZxwSqFug=","Input":null,"MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":10,"R":45333509585443656633951985470710690691223375027582566090805878773707210656245,"S":35038432671237117773348115171747112083263101921428552308743969513077292471285,"Status":1,"To":"PR8ike+t23Co25Ogb5DyI8scvGE=","V":38,"Value":1560000000000000000},{"AccessList":null,"BlockHeight":13128207,"BlockHash":"0so3irXwfF8vIk6Tmr8hnwezfdCa7bYggxwhIr5sukk=","ContractAddress":null,"Index":45,"Type":0,"From":"cYPfo3o+AAReGaTJn6Y7keIEAok=","GasLimit":42000,"GasPrice":130000000000,"GasUsed":21000,"Hash":"nNlCjPaulL4US6p0G73U16PpS72TfT+yUgNIu9IpAWM=","Input":null,"MaxFeePerGas":null,"MaxPriorityFeePerGas":null,"Nonce":11,"R":37346395048077141134769707734256673333803990711984565840776690161793139423671,"S":53630568011174242505513599407303774336955279979846214045971519389459024884363,"Status":1,"To":"PR8ike+t23Co25Ogb5DyI8scvGE=","V":38,"Value":92000000000000000}]`,
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
