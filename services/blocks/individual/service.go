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

package individual

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/attestantio/go-execution-client/spec"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/execd/services/execdb"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	chainHeightProvider         execclient.ChainHeightProvider
	blocksProvider              execclient.BlocksProvider
	blockReplaysProvider        execclient.BlockReplaysProvider
	issuanceProvider            execclient.IssuanceProvider
	transactionReceiptsProvider execclient.TransactionReceiptsProvider
	blocksSetter                execdb.BlocksSetter
	transactionsSetter          execdb.TransactionsSetter
	transactionStateDiffsSetter execdb.TransactionStateDiffsSetter
	eventsSetter                execdb.EventsSetter
	activitySem                 *semaphore.Weighted
	enableTransactions          bool
	enableTransactionEvents     bool
	enableBalanceChanges        bool
	enableStorageChanges        bool
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "blocks").Str("impl", "individual").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		chainHeightProvider:         parameters.chainHeightProvider,
		blocksProvider:              parameters.blocksProvider,
		blockReplaysProvider:        parameters.blockReplaysProvider,
		issuanceProvider:            parameters.issuanceProvider,
		transactionReceiptsProvider: parameters.transactionReceiptsProvider,
		blocksSetter:                parameters.blocksSetter,
		transactionsSetter:          parameters.transactionsSetter,
		transactionStateDiffsSetter: parameters.transactionStateDiffsSetter,
		eventsSetter:                parameters.eventsSetter,
		activitySem:                 semaphore.NewWeighted(1),
		enableTransactions:          parameters.enableTransactions,
		enableTransactionEvents:     parameters.enableTransactionEvents,
		enableBalanceChanges:        parameters.enableBalanceChanges,
		enableStorageChanges:        parameters.enableStorageChanges,
	}

	// Update to current block before starting (in the background).
	go s.updateAfterRestart(ctx, parameters.startHeight)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startHeight int64) {
	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startHeight >= 0 {
		// Explicit requirement to start at a given height.
		// Subtract one to state that the block higher than the required start is the last processed.
		md.LatestHeight = startHeight - 1
	}

	log.Info().Int64("height", md.LatestHeight).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Info().Msg("Caught up")

	//	// Set up the handler for new chain head updates.
	//	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"head"}, func(event *api.Event) {
	//		if event.Data == nil {
	//			// Happens when the channel shuts down, nothing to worry about.
	//			return
	//		}
	//		eventData := event.Data.(*api.HeadEvent)
	//		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	//	}); err != nil {
	//		log.Fatal().Err(err).Msg("Failed to add beacon chain head updated handler")
	//	}
}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	chainHeight, err := s.chainHeightProvider.ChainHeight(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain chain height")
		return
	}
	safetyMargin := uint32(64) // Run 64 blocks behind for safety; could make this much tighter with integration With CL.
	maxHeight := chainHeight - safetyMargin
	log.Trace().Uint32("chain_height", chainHeight).Uint32("fetch_height", maxHeight).Msg("Fetch parameters")

	for height := uint32(md.LatestHeight + 1); height <= maxHeight; height++ {
		log.Trace().Uint32("height", height).Msg("Fetching block")
		block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", height))
		if err != nil {
			log.Error().Err(err).Msg("Failed to obtain block")
			return
		}

		if err := s.handleBlock(ctx, md, block); err != nil {
			log.Error().Err(err).Uint32("height", block.London.Number).Str("hash", fmt.Sprintf("%#x", block.London.Hash)).Msg("Failed to handle block")
			return
		}
	}
}

func (s *Service) handleBlock(ctx context.Context,
	md *metadata,
	block *spec.Block,
) error {
	ctx, cancel, err := s.blocksSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}
	log.Trace().Uint32("height", block.London.Number).Str("hash", fmt.Sprintf("%#x", block.London.Hash)).Msg("Obtained block from client")

	dbBlock := &execdb.Block{
		Height:          block.London.Number,
		Hash:            block.London.Hash,
		BaseFee:         block.London.BaseFeePerGas,
		Difficulty:      block.London.Difficulty,
		ExtraData:       block.London.ExtraData,
		GasLimit:        block.London.GasLimit,
		GasUsed:         block.London.GasUsed,
		FeeRecipient:    block.London.Miner,
		ParentHash:      block.London.ParentHash,
		Size:            block.London.Size,
		StateRoot:       block.London.StateRoot,
		Timestamp:       block.London.Timestamp,
		TotalDifficulty: block.London.TotalDifficulty,
	}

	log.Warn().Msg(fmt.Sprintf("1 - %v", s.issuanceProvider != nil))
	if s.issuanceProvider != nil {
		log.Warn().Msg("2")
		issuance, err := s.issuanceProvider.Issuance(ctx, fmt.Sprintf("%d", block.London.Number))
		if err != nil {
			return err
		}
		log.Warn().Str("issuance", fmt.Sprintf("%v", issuance)).Msg("3")
		dbBlock.Issuance = issuance.Issuance
	}

	if err := s.blocksSetter.SetBlock(ctx, dbBlock); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set block")
	}

	if err := s.handleBlockTransactions(ctx, md, block); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set block transactions")
	}
	md.LatestHeight = int64(block.London.Number)

	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.blocksSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}
	monitorBlockProcessed(block.London.Number)

	return nil
}

func (s *Service) handleBlockTransactions(ctx context.Context,
	md *metadata,
	block *spec.Block,
) error {
	dbTransactions := make([]*execdb.Transaction, 0, len(block.London.Transactions))
	// Guess at initial capacity here...
	dbEvents := make([]*execdb.Event, 0, len(block.London.Transactions)*4)
	for i, transaction := range block.London.Transactions {
		var dbTransaction *execdb.Transaction
		switch tx := transaction.(type) {
		case *spec.Type0Transaction:
			dbTransaction = s.compileType0Transaction(ctx, block, tx, i)
		case *spec.Type1Transaction:
			dbTransaction = s.compileType1Transaction(ctx, block, tx, i)
		case *spec.Type2Transaction:
			dbTransaction = s.compileType2Transaction(ctx, block, tx, i)
		default:
			log.Warn().Uint64("type", transaction.Type()).Msg("Unhandled transaction type")
			continue
		}

		dbTxEvents, err := s.addTransactionReceiptInfo(ctx, dbTransaction)
		if err != nil {
			return errors.Wrap(err, "failed to add receipt to transaction")
		}

		dbTransactions = append(dbTransactions, dbTransaction)
		dbEvents = append(dbEvents, dbTxEvents...)
	}

	if s.enableTransactionEvents {
		if err := s.transactionsSetter.SetTransactions(ctx, dbTransactions); err != nil {
			return errors.Wrap(err, "failed to set transactions")
		}
	}

	if s.enableTransactionEvents {
		if err := s.eventsSetter.SetEvents(ctx, dbEvents); err != nil {
			return errors.Wrap(err, "failed to set events")
		}
	}

	if s.enableBalanceChanges || s.enableStorageChanges {
		transactionsResults, err := s.blockReplaysProvider.ReplayBlockTransactions(ctx, fmt.Sprintf("%d", block.London.Number))
		if err != nil {
			return errors.Wrap(err, "failed to replay block")
		}

		// Guess at initial capacity here...
		dbStateDiffs := make([]*execdb.TransactionStateDiff, 0, len(block.London.Transactions)*4)
		for _, transactionResults := range transactionsResults {
			dbStateDiff := &execdb.TransactionStateDiff{
				BalanceChanges: make([]*execdb.TransactionBalanceChange, 0, len(transactionResults.StateDiff)),
			}
			for key, stateDiff := range transactionResults.StateDiff {
				address, err := hex.DecodeString(strings.TrimPrefix(key, "0x"))
				if err != nil {
					return errors.Wrap(err, "invalid state diff address")
				}
				if stateDiff.Balance != nil {
					dbBalanceChange := &execdb.TransactionBalanceChange{
						TransactionHash: transactionResults.TransactionHash,
						BlockHeight:     block.London.Number,
						Address:         address,
						Old:             stateDiff.Balance.From,
						New:             stateDiff.Balance.To,
					}
					dbStateDiff.BalanceChanges = append(dbStateDiff.BalanceChanges, dbBalanceChange)
				}

				if stateDiff.Storage != nil {
					for storageHashStr, stateChange := range stateDiff.Storage {
						storageHash, err := hex.DecodeString(strings.TrimPrefix(storageHashStr, "0x"))
						if err != nil {
							return errors.Wrap(err, "invalid storage change storage hash")
						}
						dbStorageChange := &execdb.TransactionStorageChange{
							TransactionHash: transactionResults.TransactionHash,
							BlockHeight:     block.London.Number,
							Address:         address,
							StorageAddress:  storageHash,
							Value:           stateChange.To,
						}
						dbStateDiff.StorageChanges = append(dbStateDiff.StorageChanges, dbStorageChange)
					}
				}
			}
			dbStateDiffs = append(dbStateDiffs, dbStateDiff)
		}

		if err := s.transactionStateDiffsSetter.SetTransactionStateDiffs(ctx, dbStateDiffs); err != nil {
			return errors.Wrap(err, "failed to set state differences")
		}
	}

	return nil
}

func (s *Service) compileType0Transaction(ctx context.Context,
	block *spec.Block,
	tx *spec.Type0Transaction,
	index int,
) *execdb.Transaction {
	return &execdb.Transaction{
		BlockHeight: block.London.Number,
		BlockHash:   tx.BlockHash,
		Index:       uint32(index),
		Type:        0,
		From:        tx.From,
		GasLimit:    tx.Gas,
		GasPrice:    &tx.GasPrice,
		// Gas used comes from receipt.
		Hash:  tx.Hash,
		Input: tx.Input,
		Nonce: tx.Nonce,
		R:     tx.R,
		S:     tx.S,
		// Status comes from receipt.
		To:    tx.To,
		V:     tx.V,
		Value: tx.Value,
	}
}

func (s *Service) compileType1Transaction(ctx context.Context,
	block *spec.Block,
	tx *spec.Type1Transaction,
	index int,
) *execdb.Transaction {
	accessList := make(map[string][][]byte)
	for _, entry := range tx.AccessList {
		accessList[fmt.Sprintf("%#x", entry.Address)] = entry.StorageKeys
	}
	return &execdb.Transaction{
		AccessList:  accessList,
		BlockHeight: block.London.Number,
		BlockHash:   tx.BlockHash,
		Index:       uint32(index),
		Type:        1,
		From:        tx.From,
		GasLimit:    tx.Gas,
		GasPrice:    &tx.GasPrice,
		// Gas used comes from receipt.
		Hash:  tx.Hash,
		Input: tx.Input,
		Nonce: tx.Nonce,
		R:     tx.R,
		S:     tx.S,
		// Status comes from receipt.
		To:    tx.To,
		V:     tx.V,
		Value: tx.Value,
	}
}

func (s *Service) compileType2Transaction(ctx context.Context,
	block *spec.Block,
	tx *spec.Type2Transaction,
	index int,
) *execdb.Transaction {
	accessList := make(map[string][][]byte)
	for _, entry := range tx.AccessList {
		accessList[fmt.Sprintf("%#x", entry.Address)] = entry.StorageKeys
	}
	return &execdb.Transaction{
		AccessList:  accessList,
		BlockHeight: block.London.Number,
		BlockHash:   tx.BlockHash,
		Index:       uint32(index),
		Type:        2,
		From:        tx.From,
		GasLimit:    tx.Gas,
		Hash:        tx.Hash,
		Input:       tx.Input,
		// Gas used comes from receipt.
		MaxFeePerGas:         &tx.MaxFeePerGas,
		MaxPriorityFeePerGas: &tx.MaxPriorityFeePerGas,
		Nonce:                tx.Nonce,
		R:                    tx.R,
		S:                    tx.S,
		// Status comes from receipt.
		To:    tx.To,
		V:     tx.V,
		Value: tx.Value,
	}
}

func (s *Service) addTransactionReceiptInfo(ctx context.Context,
	dbTransaction *execdb.Transaction,
) (
	[]*execdb.Event,
	error,
) {
	receipt, err := s.transactionReceiptsProvider.TransactionReceipt(ctx, dbTransaction.Hash)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain transaction receipt")
	}
	if receipt.GasUsed > 0 {
		dbTransaction.GasUsed = receipt.GasUsed
		dbTransaction.Status = receipt.Status
	}

	dbEvents := make([]*execdb.Event, 0, len(receipt.Logs))
	for _, event := range receipt.Logs {
		dbEvents = append(dbEvents, &execdb.Event{
			TransactionHash: dbTransaction.Hash,
			BlockHeight:     receipt.BlockNumber,
			Index:           event.Index,
			Address:         event.Address,
			Topics:          event.Topics,
			Data:            event.Data,
		})
	}

	return dbEvents, nil
}
