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
	"fmt"

	"github.com/attestantio/go-execution-client/spec"
	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
)

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
			log.Error().Err(err).Uint32("height", block.Number()).Str("hash", fmt.Sprintf("%#x", block.Hash())).Msg("Failed to handle block")
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
	log.Trace().Uint32("height", block.Number()).Str("hash", fmt.Sprintf("%#x", block.Hash())).Msg("Obtained block from client")

	hash := block.Hash()
	miner := block.Miner()
	parentHash := block.ParentHash()
	stateRoot := block.StateRoot()
	dbBlock := &execdb.Block{
		Height:          block.Number(),
		Hash:            hash[:],
		BaseFee:         block.BaseFeePerGas(),
		Difficulty:      block.Difficulty(),
		ExtraData:       block.ExtraData(),
		GasLimit:        block.GasLimit(),
		GasUsed:         block.GasUsed(),
		FeeRecipient:    miner[:],
		ParentHash:      parentHash[:],
		Size:            block.Size(),
		StateRoot:       stateRoot[:],
		Timestamp:       block.Timestamp(),
		TotalDifficulty: block.TotalDifficulty(),
	}

	if s.issuanceProvider != nil {
		issuance, err := s.issuanceProvider.Issuance(ctx, fmt.Sprintf("%d", block.Number()))
		if err != nil {
			return err
		}
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
	md.LatestHeight = int64(block.Number())

	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.blocksSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}
	monitorBlockProcessed(block.Number())

	return nil
}

func (s *Service) handleBlockTransactions(ctx context.Context,
	md *metadata,
	block *spec.Block,
) error {
	dbTransactions := make([]*execdb.Transaction, 0, len(block.Transactions()))
	// Guess at initial capacity here...
	dbEvents := make([]*execdb.Event, 0, len(block.Transactions())*4)
	for i, transaction := range block.Transactions() {
		dbTransaction := s.compileTransaction(ctx, block, transaction, i)

		dbTxEvents, err := s.addTransactionReceiptInfo(ctx, transaction, dbTransaction)
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
		transactionsResults, err := s.blockReplaysProvider.ReplayBlockTransactions(ctx, fmt.Sprintf("%d", block.Number()))
		if err != nil {
			return errors.Wrap(err, "failed to replay block")
		}

		// Guess at initial capacity here...
		dbStateDiffs := make([]*execdb.TransactionStateDiff, 0, len(block.Transactions())*4)
		for _, transactionResults := range transactionsResults {
			dbStateDiff := &execdb.TransactionStateDiff{
				BalanceChanges: make([]*execdb.TransactionBalanceChange, 0, len(transactionResults.StateDiff)),
			}
			for k, stateDiff := range transactionResults.StateDiff {
				address := k
				if stateDiff.Balance != nil {
					dbBalanceChange := &execdb.TransactionBalanceChange{
						TransactionHash: transactionResults.TransactionHash[:],
						BlockHeight:     block.Number(),
						Address:         address[:],
						Old:             stateDiff.Balance.From,
						New:             stateDiff.Balance.To,
					}
					dbStateDiff.BalanceChanges = append(dbStateDiff.BalanceChanges, dbBalanceChange)
				}

				if stateDiff.Storage != nil {
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

		if err := s.transactionStateDiffsSetter.SetTransactionStateDiffs(ctx, dbStateDiffs); err != nil {
			return errors.Wrap(err, "failed to set state differences")
		}
	}

	return nil
}

func (s *Service) compileTransaction(ctx context.Context,
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
	if tx.Type == 1 || tx.Type == 2 {
		dbTransaction.AccessList = make(map[string][][]byte)
		for _, entry := range tx.AccessList() {
			dbTransaction.AccessList[fmt.Sprintf("%x", entry.Address)] = entry.StorageKeys
		}
	}
	if tx.Type == 2 {
		maxFeePerGas := tx.MaxFeePerGas()
		dbTransaction.MaxFeePerGas = &maxFeePerGas
		maxPriorityFeePerGas := tx.MaxPriorityFeePerGas()
		dbTransaction.MaxPriorityFeePerGas = &maxPriorityFeePerGas
		// Calculate the gas price.
		feePerGas := maxPriorityFeePerGas
		if maxPriorityFeePerGas > maxFeePerGas-block.BaseFeePerGas() {
			feePerGas = maxFeePerGas - block.BaseFeePerGas()
		}
		dbTransaction.GasPrice = feePerGas
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
