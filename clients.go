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

package main

import (
	"context"
	"sync"

	execclient "github.com/attestantio/go-execution-client"
	jsonrpc "github.com/attestantio/go-execution-client/jsonrpc"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wealdtech/execd/util"
)

var (
	clients   map[string]execclient.Service
	clientsMu sync.Mutex
)

// fetchClient fetches a client service, instantiating it if required.
func fetchClient(ctx context.Context, address string) (execclient.Service, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	if clients == nil {
		clients = make(map[string]execclient.Service)
	}

	var client execclient.Service
	var exists bool
	if client, exists = clients[address]; !exists {
		var err error
		client, err = jsonrpc.New(ctx,
			jsonrpc.WithLogLevel(util.LogLevel("execclient")),
			jsonrpc.WithTimeout(viper.GetDuration("execclient.timeout")),
			jsonrpc.WithAddress(address))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initiate client")
		}
		// Confirm that the client provides the required interfaces.
		if err := confirmClientInterfaces(ctx, client); err != nil {
			return nil, errors.Wrap(err, "missing required interface")
		}
		clients[address] = client
	}

	return client, nil
}

func confirmClientInterfaces(_ context.Context, client execclient.Service) error {
	if _, isProvider := client.(execclient.BlocksProvider); !isProvider {
		return errors.New("client is not a BlocksProvider")
	}

	return nil
}
