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
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/pkg/errors"
	"github.com/wealdtech/execd/services/execdb"
)

func (s *Service) catchup(ctx context.Context, md *metadata) {
	// Obtain the height of the latest block in the DB.
	blocks, err := s.blocksProvider.Blocks(ctx, &execdb.BlockFilter{
		Order: execdb.OrderLatest,
		Limit: 1,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain block for chain height")
		return
	}
	if len(blocks) == 0 {
		// No blocks means nothing to do.
		return
	}
	maxHeight := blocks[0].Height
	log.Trace().Uint32("max_height", maxHeight).Msg("Update parameters")

	// Calculate total fees and payments for each block, writing out in a batch.
	batchSize := 512
	blockRewards := make([]*execdb.BlockReward, 0, 512)
	for height := uint32(md.LatestHeight + 1); height <= maxHeight; height++ {
		blockReward, err := s.handleBlock(ctx, height)
		if err != nil {
			log.Error().Err(err).Uint32("height", height).Msg("Failed to handle block")
		}
		if blockReward == nil {
			continue
		}
		blockRewards = append(blockRewards, blockReward)
		if len(blockRewards) == batchSize {
			if err := s.store(ctx, md, blockRewards); err != nil {
				log.Error().Err(err).Msg("Failed to store rewards")
				return
			}
			log.Trace().Uint32("height", height).Msg("Batch store")
			blockRewards = blockRewards[:0]
		}
	}
	if len(blockRewards) > 0 {
		if err := s.store(ctx, md, blockRewards); err != nil {
			log.Error().Err(err).Msg("Failed to store final rewards")
			return
		}
	}
}

func (s *Service) handleBlock(ctx context.Context,
	height uint32,
) (
	*execdb.BlockReward,
	error,
) {
	blockFees := uint256.NewInt(0)
	blockPayments := uint256.NewInt(0)
	blocks, err := s.blocksProvider.Blocks(ctx, &execdb.BlockFilter{
		From: &height,
		To:   &height,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain block")
	}
	if len(blocks) == 0 {
		return nil, errors.New("no block")
	}
	block := blocks[0]

	transactions, err := s.transactionsProvider.Transactions(ctx, &execdb.TransactionFilter{
		From: &height,
		To:   &height,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain transactions")
	}
	for _, transaction := range transactions {
		txFees, txPayments, err := s.handleTransaction(ctx, block, transaction)
		if err != nil {
			return nil, errors.Wrap(err, "failed to handle transaction")
		}
		blockFees.Add(blockFees, txFees)
		blockPayments.Add(blockPayments, txPayments)
	}

	blockReward := &execdb.BlockReward{
		BlockHash:   block.Hash,
		BlockHeight: block.Height,
		Fees:        blockFees,
		Payments:    blockPayments,
	}

	return blockReward, nil
}

func (s *Service) handleTransaction(ctx context.Context,
	block *execdb.Block,
	transaction *execdb.Transaction,
) (
	*uint256.Int,
	*uint256.Int,
	error,
) {
	log := log.With().Str("transaction_hash", fmt.Sprintf("%#x", transaction.Hash)).Uint64("type", transaction.Type).Logger()
	zero := big.NewInt(0)

	var feePerGas *uint256.Int
	if transaction.Type >= 2 {
		feePerGas = uint256.NewInt(*transaction.MaxPriorityFeePerGas)
		if *transaction.MaxPriorityFeePerGas > *transaction.MaxFeePerGas-block.BaseFee {
			feePerGas = uint256.NewInt(*transaction.MaxFeePerGas - block.BaseFee)
		}
	} else {
		feePerGas = uint256.NewInt(transaction.GasPrice - block.BaseFee)
	}
	fees := feePerGas.Mul(feePerGas, uint256.NewInt(uint64(transaction.GasUsed)))
	log.Trace().Uint64("gas_price", transaction.GasPrice).Uint32("gas_used", transaction.GasUsed).Uint64("base_fee", block.BaseFee).Stringer("fee_per_gas", feePerGas).Stringer("fees", fees).Msg("Fee per gas")

	transactionStateDiff, err := s.transactionStateDiffsProvider.TransactionStateDiff(ctx, transaction.Hash)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to obtain transaction state diff")
	}

	payments := uint256.NewInt(0)
	for _, balanceChange := range transactionStateDiff.BalanceChanges {
		if bytes.Equal(balanceChange.Address, block.FeeRecipient) {
			delta := new(big.Int).Sub(new(big.Int).Sub(balanceChange.New, balanceChange.Old), fees.ToBig())
			if delta.Cmp(zero) > 0 {
				// There was a direct payment to the miner as well as the transaction fee.
				d, overflow := uint256.FromBig(delta)
				if overflow {
					return nil, nil, errors.New("overflow")
				}
				payments = payments.Add(payments, d)
			}
			break
		}
	}

	log.Trace().Uint32("height", block.Height).Str("transaction_hash", fmt.Sprintf("%#x", transaction.Hash)).Stringer("fees", fees).Stringer("payments", payments).Msg("Calculated transaction fees and payments")

	return fees, payments, nil
}

func (s *Service) store(ctx context.Context,
	md *metadata,
	blockRewards []*execdb.BlockReward,
) error {
	ctx, cancel, err := s.blockRewardsSetter.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.blockRewardsSetter.SetBlockRewards(ctx, blockRewards); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set rewards")
	}

	md.LatestHeight = int64(blockRewards[len(blockRewards)-1].BlockHeight)
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.blockRewardsSetter.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	monitorBlockProcessed(blockRewards[len(blockRewards)-1].BlockHeight)

	return nil
}
