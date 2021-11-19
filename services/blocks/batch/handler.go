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

package batch

import (
	"bytes"
	"context"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	stateRoot phase0.Root,
	epochTransition bool,
) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	log := log.With().Uint64("slot", uint64(slot)).Str("block_root", fmt.Sprintf("%#x", blockRoot)).Logger()
	log.Trace().
		Str("state_root", fmt.Sprintf("%#x", stateRoot)).
		Bool("epoch_transition", epochTransition).
		Msg("Handler called")

	if bytes.Equal(s.lastHandledBlockRoot, blockRoot[:]) {
		log.Trace().Msg("Already handled this block; ignoring")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	s.catchup(ctx, md)

	//	s.lastHandledBlockRoot = blockRoot
	//	monitorBlockProcessed(slot)
}

// OnBlock handles a block.
// This requires the context to hold an active transaction.
func (s *Service) OnBlock(ctx context.Context, hash []byte) error {
	return nil
}
