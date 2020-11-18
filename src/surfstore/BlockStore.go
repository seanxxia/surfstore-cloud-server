package surfstore

import (
	"errors"
)

type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	block, ok := bs.BlockMap[blockHash]
	if !ok {
		return errors.New("block not found")
	}

	*blockData = block
	return nil

}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	bs.BlockMap[block.Hash()] = block
	*succ = true
	return nil
}

func (bs *BlockStore) HasBlocks(blockHashList []string, existedBlockHashList *[]string) error {
	for _, blockHash := range blockHashList {
		if _, ok := bs.BlockMap[blockHash]; ok {
			*existedBlockHashList = append(*existedBlockHashList, blockHash)
		}
	}

	return nil
}

func (bs *BlockStore) HasBlock(blockHash string, succ *bool) error {
	_, *succ = bs.BlockMap[blockHash]
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
