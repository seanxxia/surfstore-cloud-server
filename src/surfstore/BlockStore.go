package surfstore

import (
	"crypto/sha256"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]Block
}

func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	// panic("todo")
	value, ok := bs.BlockMap[blockHash]
	if ok {
		*blockData = value
		return nil
	} else {
		return errors.New("block not found")
	}

}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	// panic("todo")
	if block.BlockSize == 0 {
		*succ = false
		return errors.New("empty block")
	}
	h := sha256.Sum256(block.BlockData)
	bs.BlockMap[string(h[:])] = block
	*succ = true
	return nil
}

func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// panic("todo")
	for _, h := range blockHashesIn {
		_, ok := bs.BlockMap[h]
		if ok {
			*blockHashesOut = append(*blockHashesOut, h)
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
