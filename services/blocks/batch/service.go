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
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	execclient "github.com/attestantio/go-execution-client"
	"github.com/attestantio/go-execution-client/spec"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/util"
	"go.uber.org/atomic"
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
	lastHandledBlockRoot        []byte
	activitySem                 *semaphore.Weighted
	enableTransactions          bool
	enableTransactionEvents     bool
	enableBalanceChanges        bool
	enableStorageChanges        bool
	processConcurrency          int64
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
	log = zerologger.With().Str("service", "blocks").Str("impl", "batch").Logger().Level(parameters.logLevel)

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
		processConcurrency:          parameters.processConcurrency,
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

	// We processing of a subsequent batch to occur whilst storage of batch is taking place.  Use a mutex
	// to ensure that only one write is happening at a time.
	var writeMutex sync.Mutex
	// If the store operation fails we need to know about it.
	var storeFailed atomic.Bool
	storeFailed.Store(false)

	// Batch process the updates, with processConcurrency blocks in each batch.
	for height := uint32(md.LatestHeight + 1); height <= maxHeight; height += uint32(s.processConcurrency) {
		if storeFailed.Load() {
			return
		}

		entries := uint32(s.processConcurrency)
		if height+entries > maxHeight {
			entries = maxHeight - height
		}

		blockHeights := make([]uint32, entries)
		for i := uint32(0); i < entries; i++ {
			blockHeights[i] = height + i
		}
		blocks := make([]*execdb.Block, entries)
		transactionsMap := make(map[uint32][]*execdb.Transaction)
		eventsMap := make(map[uint32][]*execdb.Event)
		stateDiffsMap := make(map[uint32][]*execdb.TransactionStateDiff)
		if _, err = util.Scatter(int(entries), int(s.processConcurrency), func(offset int, entries int, mu *sync.RWMutex) (interface{}, error) {
			for i := offset; i < offset+entries; i++ {
				log.Trace().Uint32("height", blockHeights[i]).Msg("Fetching block")
				block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", blockHeights[i]))
				if err != nil {
					return nil, err
				}
				if err := s.handleBlock(ctx, md, mu, blocks, i, transactionsMap, eventsMap, stateDiffsMap, block); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}); err != nil {
			log.Error().Err(err).Msg("Failed to batch fetch blocks")
			return
		}

		// Turn maps in to arrays.
		transactions := make([]*execdb.Transaction, 0)
		for _, v := range transactionsMap {
			transactions = append(transactions, v...)
		}
		events := make([]*execdb.Event, 0)
		for _, v := range eventsMap {
			events = append(events, v...)
		}
		stateDiffs := make([]*execdb.TransactionStateDiff, 0)
		for _, v := range stateDiffsMap {
			stateDiffs = append(stateDiffs, v...)
		}

		go func(ctx context.Context,
			md *metadata,
			blocks []*execdb.Block,
			transactions []*execdb.Transaction,
			events []*execdb.Event,
			stateDiffs []*execdb.TransactionStateDiff,
		) {
			writeMutex.Lock()
			if storeFailed.Load() {
				return
			}
			log.Trace().Int("blocks", len(blocks)).Int("transaction", len(transactions)).Int("events", len(events)).Int("state_diffs", len(stateDiffs)).Msg("Items to store")
			if err := s.store(ctx, md, blocks, transactions, events, stateDiffs); err != nil {
				log.Error().Err(err).Msg("Failed to store")
				storeFailed.Store(true)
				writeMutex.Unlock()
				return
			}
			writeMutex.Unlock()
		}(ctx, md, blocks, transactions, events, stateDiffs)
	}
}

func (s *Service) handleBlock(ctx context.Context,
	md *metadata,
	mu *sync.RWMutex,
	blocks []*execdb.Block,
	blockOffset int,
	transactions map[uint32][]*execdb.Transaction,
	events map[uint32][]*execdb.Event,
	stateDiffs map[uint32][]*execdb.TransactionStateDiff,
	block *spec.Block,
) error {
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

	if s.issuanceProvider != nil {
		issuance, err := s.issuanceProvider.Issuance(ctx, fmt.Sprintf("%d", block.London.Number))
		if err != nil {
			return err
		}
		dbBlock.Issuance = issuance.Issuance
	}

	dbTransactions, dbEvents, dbStateDiffs, err := s.fetchBlockTransactions(ctx, block)
	if err != nil {
		return errors.Wrap(err, "failed to fetch block transactions")
	}

	mu.Lock()
	blocks[blockOffset] = dbBlock
	transactions[dbBlock.Height] = dbTransactions
	events[dbBlock.Height] = dbEvents
	stateDiffs[dbBlock.Height] = dbStateDiffs
	mu.Unlock()

	return nil
}

func (s *Service) fetchBlockTransactions(ctx context.Context,
	block *spec.Block,
) (
	[]*execdb.Transaction,
	[]*execdb.Event,
	[]*execdb.TransactionStateDiff,
	error,
) {
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
			return nil, nil, nil, errors.Wrap(err, "failed to add receipt to transaction")
		}

		dbTransactions = append(dbTransactions, dbTransaction)
		dbEvents = append(dbEvents, dbTxEvents...)
	}

	// Guess at initial capacity here...
	dbStateDiffs := make([]*execdb.TransactionStateDiff, 0, len(block.London.Transactions)*4)
	if s.enableBalanceChanges || s.enableStorageChanges {
		transactionsResults, err := s.blockReplaysProvider.ReplayBlockTransactions(ctx, fmt.Sprintf("%d", block.London.Number))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to replay block")
		}

		for _, transactionResults := range transactionsResults {
			dbStateDiff := &execdb.TransactionStateDiff{
				BalanceChanges: make([]*execdb.TransactionBalanceChange, 0, len(transactionResults.StateDiff)),
			}
			for key, stateDiff := range transactionResults.StateDiff {
				address, err := hex.DecodeString(strings.TrimPrefix(key, "0x"))
				if err != nil {
					return nil, nil, nil, errors.Wrap(err, "invalid state diff address")
				}

				if s.enableBalanceChanges && stateDiff.Balance != nil {
					dbBalanceChange := &execdb.TransactionBalanceChange{
						TransactionHash: transactionResults.TransactionHash,
						BlockHeight:     block.London.Number,
						Address:         address,
						Old:             stateDiff.Balance.From,
						New:             stateDiff.Balance.To,
					}
					dbStateDiff.BalanceChanges = append(dbStateDiff.BalanceChanges, dbBalanceChange)
				}

				if s.enableStorageChanges && stateDiff.Storage != nil {
					for storageHashStr, stateChange := range stateDiff.Storage {
						storageHash, err := hex.DecodeString(strings.TrimPrefix(storageHashStr, "0x"))
						if err != nil {
							return nil, nil, nil, errors.Wrap(err, "invalid storage change storage hash")
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
	}

	return dbTransactions, dbEvents, dbStateDiffs, nil
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

func (s *Service) store(ctx context.Context,
	md *metadata,
	blocks []*execdb.Block,
	transactions []*execdb.Transaction,
	events []*execdb.Event,
	stateDiffs []*execdb.TransactionStateDiff,
) error {
	ctx, cancel, err := s.blocksSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.blocksSetter.SetBlocks(ctx, blocks); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set blocks")
	}

	if err := s.transactionsSetter.SetTransactions(ctx, transactions); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set transactions")
	}

	if err := s.eventsSetter.SetEvents(ctx, events); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set events")
	}

	if err := s.transactionStateDiffsSetter.SetTransactionStateDiffs(ctx, stateDiffs); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set state diffs")
	}

	md.LatestHeight = int64(blocks[len(blocks)-1].Height)
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}
	monitorBlockProcessed(blocks[len(blocks)-1].Height)

	if err := s.blocksSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}
