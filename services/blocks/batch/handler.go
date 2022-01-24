// Copyright © 2021, 2022 Weald Technology Trading.
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
	safetyMargin := uint32(64) // Run 64 blocks behind for safety; could make this much tighter with integration With CL.
	maxHeight := chainHeight - safetyMargin
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
		for i := uint32(0); i < entries; i++ {
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
					return nil, err
				}
				if err := s.handleBlock(ctx, md, mu, bd, i, block); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}); err != nil {
			log.Error().Err(err).Msg("Failed to batch fetch blocks")
			return
		}

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
	md *metadata,
	mu *sync.RWMutex,
	bd *batchData,
	blockOffset int,
	block *spec.Block,
) error {
	dbBlock := &execdb.Block{
		Height:          block.London.Number,
		Hash:            block.London.Hash[:],
		BaseFee:         block.London.BaseFeePerGas,
		Difficulty:      block.London.Difficulty,
		ExtraData:       block.London.ExtraData,
		GasLimit:        block.London.GasLimit,
		GasUsed:         block.London.GasUsed,
		FeeRecipient:    block.London.Miner[:],
		ParentHash:      block.London.ParentHash[:],
		Size:            block.London.Size,
		StateRoot:       block.London.StateRoot[:],
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
		return errors.Wrap(err, fmt.Sprintf("failed to fetch transactions for block %#x", block.London.Hash))
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
	dbTransactions := make([]*execdb.Transaction, 0, len(block.London.Transactions))
	// Guess at initial capacity here...
	dbEvents := make([]*execdb.Event, 0, len(block.London.Transactions)*4)
	for i, transaction := range block.London.Transactions {
		dbTransaction := s.compileTransaction(ctx, block, transaction, i)

		dbTxEvents, err := s.addTransactionReceiptInfo(ctx, transaction, dbTransaction)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, fmt.Sprintf("failed to add receipt to transaction %#x", transaction.Hash))
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
			for k, stateDiff := range transactionResults.StateDiff {
				address := k
				if s.enableBalanceChanges && stateDiff.Balance != nil {
					dbBalanceChange := &execdb.TransactionBalanceChange{
						TransactionHash: transactionResults.TransactionHash[:],
						BlockHeight:     block.London.Number,
						Address:         address[:],
						Old:             stateDiff.Balance.From,
						New:             stateDiff.Balance.To,
					}
					dbStateDiff.BalanceChanges = append(dbStateDiff.BalanceChanges, dbBalanceChange)
				}

				if s.enableStorageChanges && stateDiff.Storage != nil {
					for l, stateChange := range stateDiff.Storage {
						storageHash := l
						dbStorageChange := &execdb.TransactionStorageChange{
							TransactionHash: transactionResults.TransactionHash[:],
							BlockHeight:     block.London.Number,
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

func (s *Service) compileTransaction(ctx context.Context,
	block *spec.Block,
	tx *spec.Transaction,
	index int,
) *execdb.Transaction {
	var to *[]byte
	if tx.To != nil {
		tmp := tx.To[:]
		to = &tmp
	}
	dbTransaction := &execdb.Transaction{
		BlockHeight: block.London.Number,
		BlockHash:   tx.BlockHash[:],
		// ContractAddress comes from receipt.
		Index:    uint32(index),
		Type:     tx.Type,
		From:     tx.From[:],
		GasLimit: tx.Gas,
		GasPrice: tx.GasPrice,
		Hash:     tx.Hash[:],
		Input:    tx.Input,
		// Gas used comes from receipt.
		Nonce: tx.Nonce,
		R:     tx.R,
		S:     tx.S,
		// Status comes from receipt.
		To:    to,
		V:     tx.V,
		Value: tx.Value,
	}
	if tx.Type == 1 || tx.Type == 2 {
		dbTransaction.AccessList = make(map[string][][]byte)
		for _, entry := range tx.AccessList {
			dbTransaction.AccessList[fmt.Sprintf("%x", entry.Address)] = entry.StorageKeys
		}
	}
	if tx.Type == 2 {
		dbTransaction.MaxFeePerGas = &tx.MaxFeePerGas
		dbTransaction.MaxPriorityFeePerGas = &tx.MaxPriorityFeePerGas
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
	receipt, err := s.transactionReceiptsProvider.TransactionReceipt(ctx, transaction.Hash)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain transaction receipt")
	}
	if receipt.GasUsed > 0 {
		dbTransaction.GasUsed = receipt.GasUsed
		dbTransaction.Status = receipt.Status
	}
	if receipt.ContractAddress != nil {
		tmp := receipt.ContractAddress[:]
		dbTransaction.ContractAddress = &tmp
	}

	dbEvents := make([]*execdb.Event, 0, len(receipt.Logs))
	for _, event := range receipt.Logs {
		topics := make([][]byte, len(event.Topics))
		for i := range event.Topics {
			topics[i] = event.Topics[i][:]
		}
		dbEvents = append(dbEvents, &execdb.Event{
			TransactionHash: dbTransaction.Hash,
			BlockHeight:     receipt.BlockNumber,
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

	monitorBlockProcessed(bd.blocks[len(bd.blocks)-1].Height)

	return nil
}
