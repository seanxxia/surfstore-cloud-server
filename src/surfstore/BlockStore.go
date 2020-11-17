package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
		err := errors.New("block not found")
		fmt.Println(err)
		return err
	}

}

func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	// panic("todo")
	hash := sha256.Sum256(block.BlockData)
	str := hex.EncodeToString(hash[:])
	bs.BlockMap[str] = block
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

func (bs *BlockStore) HasBlock(blockHash string, succ *bool) error {
	_, ok := bs.BlockMap[blockHash]
	if ok {
		*succ = true
	} else {
		*succ = false
	}

	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
