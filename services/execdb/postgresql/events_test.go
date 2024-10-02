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

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/services/execdb/postgresql"
)

func hashPtr(input string) *[]byte {
	res, err := hex.DecodeString(strings.TrimPrefix(input, "0x"))
	if err != nil {
		panic(err)
	}
	return &res
}

func hash(input string) []byte {
	res, err := hex.DecodeString(strings.TrimPrefix(input, "0x"))
	if err != nil {
		panic(err)
	}

	return res
}

func TestEvents(t *testing.T) {
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
		name   string
		filter *execdb.EventFilter
		events string
		err    string
	}{
		{
			name: "SingleBlockFirstEvent",
			filter: &execdb.EventFilter{
				Limit: 1,
				Order: execdb.OrderEarliest,
				From:  uint32Ptr(13027871),
				To:    uint32Ptr(13027871),
			},
			events: `[{"TransactionHash":"khTeZtnAXH5ZzN64bJVTsk9Ri+Sm2Bj+yhpOfMf7+7c=","BlockHeight":13027871,"Index":0,"Address":"SV+UcnZ0nOZG9orIwkhCAEXLe14=","Topics":["w9WBaMWuc5dzHQY9W789ZXhUQnND9MCDJA96rKotD2I=","AAAAAAAAAAAAAAAAUSIcoc6xjDXiDv4TcSTA0RR9jbA=","AAAAAAAAAAAAAAAAJ8NcAfkDeHXbmszH3Rpo6Rv/tIs=","AAAAAAAAAAAAAAAAvAS94vhltjw6cny4N/3N0NZMeyM="],"Data":"J8NcAfkDeHXbmszH3Rpo6Rv/tIsAAAAAAABFAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=="}]`,
		},
		{
			name: "SingleBlockLastEvent",
			filter: &execdb.EventFilter{
				Limit: 1,
				Order: execdb.OrderLatest,
				From:  uint32Ptr(13027871),
				To:    uint32Ptr(13027871),
			},
			events: `[{"TransactionHash":"khTeZtnAXH5ZzN64bJVTsk9Ri+Sm2Bj+yhpOfMf7+7c=","BlockHeight":13027871,"Index":1,"Address":"e+gHb06kpK0IB1wlCOSB1slG0Ss=","Topics":["xBCYQ+C31RTkwJMRS4Y/jn2NmkWMNyzVG/5Sa1iABsk=","AAAAAAAAAAAAAAAAJ8NcAfkDeHXbmszH3Rpo6Rv/tIs=","AAAAAAAAAAAAAAAAvAS94vhltjw6cny4N/3N0NZMeyM=","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABeXjOAprVPobib/EdTiqNhpFleAhyN6z28Q47kaUZceQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY3naBbYAA"}]`,
		},
		{
			name: "SingleBlockBothEvents",
			filter: &execdb.EventFilter{
				Limit: 2,
				Order: execdb.OrderEarliest,
				From:  uint32Ptr(13027871),
				To:    uint32Ptr(13027871),
			},
			events: `[{"TransactionHash":"khTeZtnAXH5ZzN64bJVTsk9Ri+Sm2Bj+yhpOfMf7+7c=","BlockHeight":13027871,"Index":0,"Address":"SV+UcnZ0nOZG9orIwkhCAEXLe14=","Topics":["w9WBaMWuc5dzHQY9W789ZXhUQnND9MCDJA96rKotD2I=","AAAAAAAAAAAAAAAAUSIcoc6xjDXiDv4TcSTA0RR9jbA=","AAAAAAAAAAAAAAAAJ8NcAfkDeHXbmszH3Rpo6Rv/tIs=","AAAAAAAAAAAAAAAAvAS94vhltjw6cny4N/3N0NZMeyM="],"Data":"J8NcAfkDeHXbmszH3Rpo6Rv/tIsAAAAAAABFAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=="},{"TransactionHash":"khTeZtnAXH5ZzN64bJVTsk9Ri+Sm2Bj+yhpOfMf7+7c=","BlockHeight":13027871,"Index":1,"Address":"e+gHb06kpK0IB1wlCOSB1slG0Ss=","Topics":["xBCYQ+C31RTkwJMRS4Y/jn2NmkWMNyzVG/5Sa1iABsk=","AAAAAAAAAAAAAAAAJ8NcAfkDeHXbmszH3Rpo6Rv/tIs=","AAAAAAAAAAAAAAAAvAS94vhltjw6cny4N/3N0NZMeyM=","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABeXjOAprVPobib/EdTiqNhpFleAhyN6z28Q47kaUZceQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY3naBbYAA"}]`,
		},
		{
			name: "SingleTransactionAllEvents",
			filter: &execdb.EventFilter{
				Order:           execdb.OrderEarliest,
				TransactionHash: hashPtr("0x589e8179b6aec2855b517e3841445874c133bb7e9dc453d9aea5c7df4f57b1f6"),
			},
			events: `[{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":133,"Address":"wCqqObIj/o0KDlxPJ+rZCDx1bMI=","Topics":["4f/8xJI9BLVZ9NKai/xs2gTrWw08RgdRwkAsXFzJEJw=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABotlX2fLCfBk="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":134,"Address":"YZNcvdAih7UREZ3bEa60LxWTt+8=","Topics":["aGl5Hwo0eBspiCmCzDnognaM8slplcKhEMV3xTvJMtU=","AAAAAAAAAAAAAAAAYXRvhnPuocUUaWvFI8qZH/WGTnc=","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","7WI7IfwtFeOW4BgQ4ggsPDoP8LfriT7ca4p25LcZOZs="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkAAAAAAAAAAAAAAAAAAAAAAAAB/FQvW9UxAo018PV6fVgAAAAAAAAAAAAAAAAAAAAAAAH8VC9b1TECjTXw9Xp9WAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAz5dzgiony/3LoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGi2VfZ8sJ8GQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADoCfh2gVkAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAk9HJhsAAAAAAAAAAAAAAAAGsXVHTokJTETamLlU7t6sSVJx0PAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAk9HJhsAAAAAAAAAAAAAAAAMAqqjmyI/6NCg5cTyfq2Qg8dWzCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":135,"Address":"wCqqObIj/o0KDlxPJ+rZCDx1bMI=","Topics":["3fJSrRviyJtpwrBo/DeNqpUrp/FjxKEWKPVaTfUjs+8=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y=","AAAAAAAAAAAAAAAAYXRvhnPuocUUaWvFI8qZH/WGTnc="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABotlX2fLCfBk="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":136,"Address":"axdUdOiQlMRNqYuVTu3qxJUnHQ8=","Topics":["3fJSrRviyJtpwrBo/DeNqpUrp/FjxKEWKPVaTfUjs+8=","AAAAAAAAAAAAAAAAYXRvhnPuocUUaWvFI8qZH/WGTnc=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAz5dzgiony/3Lo="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":137,"Address":"axdUdOiQlMRNqYuVTu3qxJUnHQ8=","Topics":["3fJSrRviyJtpwrBo/DeNqpUrp/FjxKEWKPVaTfUjs+8=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y=","AAAAAAAAAAAAAAAAw9A+TwQf1M04jFSe4qKanlB1iC8="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAz5dzgionz/+PI="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":138,"Address":"wCqqObIj/o0KDlxPJ+rZCDx1bMI=","Topics":["3fJSrRviyJtpwrBo/DeNqpUrp/FjxKEWKPVaTfUjs+8=","AAAAAAAAAAAAAAAAw9A+TwQf1M04jFSe4qKanlB1iC8=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABoxq1uv/WbVg="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":139,"Address":"w9A+TwQf1M04jFSe4qKanlB1iC8=","Topics":["HEEempbgcSQcLyH3cmsXronjyrTHi+UOBisDqf/7utE="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAUvAD6bCOsY8KMU4AAAAAAAAAAAAAAAAAAAAAAAAAAAAACn18ilgKn8fgwQ=="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":140,"Address":"w9A+TwQf1M04jFSe4qKanlB1iC8=","Topics":["14rZX6RsmUtlUdDahfwnX+YTzjdlf7jV49EwhAFZ2CI=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAz5dzgionz/+PIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABoxq1uv/WbVg="},{"TransactionHash":"WJ6BebauwoVbUX44QURYdMEzu36dxFPZrqXH309XsfY=","BlockHeight":12690581,"Index":141,"Address":"wCqqObIj/o0KDlxPJ+rZCDx1bMI=","Topics":["f89TLBXwptsL1tDgOL6nHTDYCMfZjLO/cmipW/UIG2U=","AAAAAAAAAAAAAAAAAAAAAAAAfxUL1vVMQKNNfD1en1Y="],"Data":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABoxq1uv/WbVg="}]`,
		},
		{
			name: "Topics",
			filter: &execdb.EventFilter{
				Order: execdb.OrderEarliest,
				Limit: 1,
				Topics: [][]byte{
					hash("0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"),
				},
			},
			events: `[{"TransactionHash":"8A+NE0Y7I20qTBvJZUlW0NY5y+5tAaiag+9KO+ZHOuY=","BlockHeight":3676978,"Index":8,"Address":"MUFZJl3Y27MQZC+Y9QwGYXPBJZs=","Topics":["zgRX/nNzH4JMwnI3YWkjUSjBGLSdNEgXQXxtEI0VXoI=","k83rcIt1RdxmjrkoAXYWnRwzz9jtbwRpCgvMiKk/xK4=","AAAEJbRGLhlGC+20vM/PFtJwl174gvA4Mb89QPc0I1U="],"Data":"AAAAAAAAAAAAAAAACQTawzR+pH0gjz/WdALQOaO5mFk="}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.Events(ctx, test.filter)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				events, err := json.Marshal(res)
				require.NoError(t, err)
				require.Equal(t, test.events, string(events))
			}
		})
	}
}
