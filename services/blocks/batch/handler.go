// Copyright © 2021 - 2023 Weald Technology Trading.
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
	"fmt"
	"strings"
	"sync"

	"github.com/attestantio/go-execution-client/spec"
	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
	"github.com/wealdtech/execd/util"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"
)

type batchData struct {
	blocks       []*execdb.Block
	transactions []*execdb.Transaction
	events       []*execdb.Event
	stateDiffs   []*execdb.TransactionStateDiff
}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	chainHeight, err := s.chainHeightProvider.ChainHeight(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain chain height")
		return
	}

	if chainHeight < s.trackDistance {
		log.Trace().Msg("Chain height not above track distance; not processing")
		return
	}
	maxHeight := chainHeight - s.trackDistance
	log.Trace().Uint32("chain_height", chainHeight).Uint32("fetch_height", maxHeight).Msg("Fetch parameters")

	// We processing of a subsequent batch to occur whilst storage of batch is taking place.  Use a semaphore
	// to ensure that only 1 write is queued at a time.
	writeSem := semaphore.NewWeighted(1)

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
			entries = maxHeight + 1 - height
		}

		blockHeights := make([]uint32, entries)
		for i := range entries {
			blockHeights[i] = height + i
		}

		bd := &batchData{
			blocks:       make([]*execdb.Block, entries),
			transactions: make([]*execdb.Transaction, 0),
			events:       make([]*execdb.Event, 0),
			stateDiffs:   make([]*execdb.TransactionStateDiff, 0),
		}

		if _, err = util.Scatter(int(entries), int(s.processConcurrency), func(offset int, entries int, mu *sync.RWMutex) (interface{}, error) {
			for i := offset; i < offset+entries; i++ {
				log.Trace().Uint32("height", blockHeights[i]).Msg("Fetching block")
				block, err := s.blocksProvider.Block(ctx, fmt.Sprintf("%d", blockHeights[i]))
				if err != nil {
					log.Debug().Uint32("height", blockHeights[i]).Msg("Failed to obtain block")
					return nil, errors.Wrap(err, "failed to obtain block")
				}
				if err := s.handleBlock(ctx, mu, bd, i, block); err != nil {
					log.Debug().Uint32("height", blockHeights[i]).Msg("Failed to handle block")
					if strings.Contains(err.Error(), "failed to replay block") {
						// Erigon can generate this error due to internal problems with its database.  Nothing we can do about it,
						// so we flag it as an error and move on.
						log.Error().Err(err).Msg("Failed to replay block, skipping and moving on")
						monitorBlockProcessed(blockHeights[i], "failed")
					} else {
						return nil, errors.Wrap(err, "failed to handle block")
					}
				}
			}
			return nil, nil
		}); err != nil {
			log.Error().Err(err).Msg("Failed to batch fetch blocks")
			return
		}

		// Remove any empty blocks from the list.
		blocks := make([]*execdb.Block, 0, entries)
		for _, block := range bd.blocks {
			if block != nil {
				blocks = append(blocks, block)
			}
		}
		bd.blocks = blocks

		if err := writeSem.Acquire(ctx, 1); err != nil {
			log.Error().Err(err).Msg("Failed to acquire write semaphore")
		}
		go func(ctx context.Context,
			md *metadata,
			bd *batchData,
		) {
			if storeFailed.Load() {
				return
			}
			log.Trace().Int("blocks", len(bd.blocks)).Int("transaction", len(bd.transactions)).Int("events", len(bd.events)).Int("state_diffs", len(bd.stateDiffs)).Msg("Items to store")
			if err := s.store(ctx, md, bd); err != nil {
				log.Error().Err(err).Msg("Failed to store")
				storeFailed.Store(true)
				writeSem.Release(1)
				return
			}
			log.Trace().Msg("Stored")
			writeSem.Release(1)
		}(ctx, md, bd)
	}
}

func (s *Service) handleBlock(ctx context.Context,
	mu *sync.RWMutex,
	bd *batchData,
	blockOffset int,
	block *spec.Block,
) error {
	hash := block.Hash()
	feeRecipient := block.FeeRecipient()
	parentHash := block.ParentHash()
	stateRoot := block.StateRoot()
	parentBeaconBlockRoot, parentBeaconBlockRootExists := block.ParentBeaconBlockRoot()
	withdrawalsRoot, withdrawalsRootExists := block.WithdrawalsRoot()
	blobGasUsed, blobGasUsedExists := block.BlobGasUsed()
	excessBlobGas, excessBlobGasExists := block.ExcessBlobGas()
	dbBlock := &execdb.Block{
		Height:          block.Number(),
		Hash:            hash[:],
		BaseFee:         block.BaseFeePerGas(),
		Difficulty:      block.Difficulty(),
		ExtraData:       block.ExtraData(),
		GasLimit:        block.GasLimit(),
		GasUsed:         block.GasUsed(),
		FeeRecipient:    feeRecipient[:],
		ParentHash:      parentHash[:],
		Size:            block.Size(),
		StateRoot:       stateRoot[:],
		Timestamp:       block.Timestamp(),
		TotalDifficulty: block.TotalDifficulty(),
	}

	if parentBeaconBlockRootExists {
		dbBlock.ParentBeaconBlockRoot = parentBeaconBlockRoot[:]
	}
	if withdrawalsRootExists {
		dbBlock.WithdrawalsRoot = withdrawalsRoot[:]
	}
	if blobGasUsedExists {
		dbBlock.BlobGasUsed = &blobGasUsed
	}
	if excessBlobGasExists {
		dbBlock.ExcessBlobGas = &excessBlobGas
	}
	if s.issuanceProvider != nil {
		issuance, err := s.issuanceProvider.Issuance(ctx, fmt.Sprintf("%d", block.Number()))
		if err != nil {
			return err
		}
		dbBlock.Issuance = issuance.Issuance
	}

	dbTransactions, dbEvents, dbStateDiffs, err := s.fetchBlockTransactions(ctx, block)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to fetch transactions for block %#x", block.Hash()))
	}

	mu.Lock()
	bd.blocks[blockOffset] = dbBlock
	bd.transactions = append(bd.transactions, dbTransactions...)
	bd.events = append(bd.events, dbEvents...)
	bd.stateDiffs = append(bd.stateDiffs, dbStateDiffs...)
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
	dbTransactions := make([]*execdb.Transaction, 0, len(block.Transactions()))
	// Guess at initial capacity here...
	dbEvents := make([]*execdb.Event, 0, len(block.Transactions())*4)
	for i, transaction := range block.Transactions() {
		dbTransaction := s.compileTransaction(ctx, block, transaction, i)

		dbTxEvents, err := s.addTransactionReceiptInfo(ctx, transaction, dbTransaction)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, fmt.Sprintf("failed to add receipt to transaction %#x", transaction.Hash()))
		}

		dbTransactions = append(dbTransactions, dbTransaction)
		dbEvents = append(dbEvents, dbTxEvents...)
	}

	// Guess at initial capacity here...
	dbStateDiffs := make([]*execdb.TransactionStateDiff, 0, len(block.Transactions())*4)
	if s.enableBalanceChanges || s.enableStorageChanges {
		transactionsResults, err := s.blockReplaysProvider.ReplayBlockTransactions(ctx, fmt.Sprintf("%d", block.Number()))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "failed to replay block")
		}

		for _, transactionResults := range transactionsResults {
			dbStateDiff := &execdb.TransactionStateDiff{
				BalanceChanges: make([]*execdb.TransactionBalanceChange, 0, len(transactionResults.StateDiff)),
			}
			for k, stateDiff := range transactionResults.StateDiff {
				address := k
				if s.enableBalanceChanges && stateDiff.Balance != nil {
					if stateDiff.Balance.From == nil || stateDiff.Balance.To == nil || stateDiff.Balance.From.Cmp(stateDiff.Balance.To) != 0 {
						dbBalanceChange := &execdb.TransactionBalanceChange{
							TransactionHash: transactionResults.TransactionHash[:],
							BlockHeight:     block.Number(),
							Address:         address[:],
							Old:             stateDiff.Balance.From,
							New:             stateDiff.Balance.To,
						}
						dbStateDiff.BalanceChanges = append(dbStateDiff.BalanceChanges, dbBalanceChange)
					}
				}

				if s.enableStorageChanges && stateDiff.Storage != nil {
					for l, stateChange := range stateDiff.Storage {
						storageHash := l
						dbStorageChange := &execdb.TransactionStorageChange{
							TransactionHash: transactionResults.TransactionHash[:],
							BlockHeight:     block.Number(),
							Address:         address[:],
							StorageAddress:  storageHash[:],
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

func (s *Service) compileTransaction(_ context.Context,
	block *spec.Block,
	tx *spec.Transaction,
	index int,
) *execdb.Transaction {
	var to *[]byte
	if tx.To() != nil {
		tmp := tx.To()
		tmpp := tmp[:]
		to = &tmpp
	}

	blockHash := tx.BlockHash()
	from := tx.From()
	hash := tx.Hash()
	dbTransaction := &execdb.Transaction{
		BlockHeight: block.Number(),
		BlockHash:   blockHash[:],
		// ContractAddress comes from receipt.
		Index:    uint32(index),
		Type:     uint64(tx.Type),
		From:     from[:],
		GasLimit: tx.Gas(),
		GasPrice: tx.GasPrice(),
		Hash:     hash[:],
		Input:    tx.Input(),
		// Gas used comes from receipt.
		Nonce: tx.Nonce(),
		R:     tx.R(),
		S:     tx.S(),
		// Status comes from receipt.
		To:    to,
		V:     tx.V(),
		Value: tx.Value(),
	}
	if tx.Type == 1 || tx.Type == 2 || tx.Type == 3 {
		dbTransaction.AccessList = make(map[string][][]byte)
		for _, entry := range tx.AccessList() {
			dbTransaction.AccessList[fmt.Sprintf("%x", entry.Address)] = entry.StorageKeys
		}
	}
	if tx.Type == 2 || tx.Type == 3 {
		maxFeePerGas := tx.MaxFeePerGas()
		dbTransaction.MaxFeePerGas = &maxFeePerGas
		maxPriorityFeePerGas := tx.MaxPriorityFeePerGas()
		dbTransaction.MaxPriorityFeePerGas = &maxPriorityFeePerGas
		// Calculate the gas price.
		priorityFeePerGas := maxFeePerGas - block.BaseFeePerGas()
		if priorityFeePerGas > maxPriorityFeePerGas {
			priorityFeePerGas = maxPriorityFeePerGas
		}
		dbTransaction.GasPrice = block.BaseFeePerGas() + priorityFeePerGas
	}
	if tx.Type == 3 {
		txBlobVersionedHashes := tx.BlobVersionedHashes()
		blobVersionedHashes := make([][]byte, len(txBlobVersionedHashes))
		for i, blobVersionedHash := range txBlobVersionedHashes {
			blobVersionedHashes[i] = blobVersionedHash[:]
		}
		dbTransaction.BlobVersionedHashes = &blobVersionedHashes
		maxFeePerGas := tx.MaxFeePerGas()
		dbTransaction.MaxFeePerGas = &maxFeePerGas
		maxPriorityFeePerGas := tx.MaxPriorityFeePerGas()
		dbTransaction.MaxPriorityFeePerGas = &maxPriorityFeePerGas
		// Calculate the gas price.
		priorityFeePerGas := maxFeePerGas - block.BaseFeePerGas()
		if priorityFeePerGas > maxPriorityFeePerGas {
			priorityFeePerGas = maxPriorityFeePerGas
		}
		dbTransaction.GasPrice = block.BaseFeePerGas() + priorityFeePerGas

		maxFeePerBlobGas := tx.MaxFeePerBlobGas()
		dbTransaction.MaxFeePerBlobGas = &maxFeePerBlobGas
		dbTransaction.BlobGasUsed = tx.BlobGasUsed()
	}

	return dbTransaction
}

func (s *Service) addTransactionReceiptInfo(ctx context.Context,
	transaction *spec.Transaction,
	dbTransaction *execdb.Transaction,
) (
	[]*execdb.Event,
	error,
) {
	receipt, err := s.transactionReceiptsProvider.TransactionReceipt(ctx, transaction.Hash())
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain transaction receipt")
	}
	if receipt.GasUsed() > 0 {
		dbTransaction.GasUsed = receipt.GasUsed()
		dbTransaction.Status = receipt.Status()
	}
	if receipt.ContractAddress() != nil {
		tmp := *receipt.ContractAddress()
		addr := tmp[:]
		dbTransaction.ContractAddress = &addr
	}
	if receipt.BlobGasPrice() != nil {
		dbTransaction.BlobGasPrice = receipt.BlobGasPrice()
	}
	if receipt.BlobGasUsed() > 0 {
		tmp := receipt.BlobGasUsed()
		dbTransaction.BlobGasUsed = &tmp
	}

	dbEvents := make([]*execdb.Event, 0, len(receipt.Logs()))
	for _, event := range receipt.Logs() {
		topics := make([][]byte, len(event.Topics))
		for i := range event.Topics {
			topics[i] = event.Topics[i][:]
		}
		dbEvents = append(dbEvents, &execdb.Event{
			TransactionHash: dbTransaction.Hash,
			BlockHeight:     receipt.BlockNumber(),
			Index:           event.Index,
			Address:         event.Address[:],
			Topics:          topics,
			Data:            event.Data,
		})
	}

	return dbEvents, nil
}

func (s *Service) store(ctx context.Context,
	md *metadata,
	bd *batchData,
) error {
	if len(bd.blocks) == 0 {
		// Nothing to store.
		return nil
	}

	ctx, cancel, err := s.blocksSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.blocksSetter.SetBlocks(ctx, bd.blocks); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set blocks")
	}

	if err := s.transactionsSetter.SetTransactions(ctx, bd.transactions); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set transactions")
	}

	if err := s.eventsSetter.SetEvents(ctx, bd.events); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set events")
	}

	if err := s.transactionStateDiffsSetter.SetTransactionStateDiffs(ctx, bd.stateDiffs); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set state diffs")
	}

	md.LatestHeight = int64(bd.blocks[len(bd.blocks)-1].Height)
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.blocksSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	for _, block := range bd.blocks {
		monitorBlockProcessed(block.Height, "succeeded")
	}

	return nil
}
