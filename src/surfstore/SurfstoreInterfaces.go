package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type Block struct {
	Data []byte
}

func NewBlock(size int) Block {
	buffer := make([]byte, size)
	return Block{Data: buffer}
}

func (block *Block) Hash() string {
	return getBytesHash(&block.Data)
}

func (block *Block) Size() int {
	return len(block.Data)
}

func getBytesHash(buffer *[]byte) string {
	hash := sha256.Sum256(*buffer)
	hashString := hex.EncodeToString(hash[:])
	return hashString
}

type FileMetaData struct {
	Filename      string
	Version       int
	BlockHashList []string
}

func (fm *FileMetaData) MarkTombstone() {
	fm.BlockHashList = []string{"0"}
}

func (fm *FileMetaData) IsTombstone() bool {
	return len(fm.BlockHashList) == 1 && fm.BlockHashList[0] == "0"
}

type Surfstore interface {
	MetaStoreInterface
	BlockStoreInterface
}

type MetaStoreInterface interface {
	// Retrieves the server's FileInfoMap
	GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error

	// Update a file's fileinfo entry
	UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error)
}

type BlockStoreInterface interface {

	// Get a block based on its hash
	GetBlock(blockHash string, block *Block) error

	// Put a block
	PutBlock(block Block, succ *bool) error

	// Check if a certain block is alredy present on the server
	HasBlock(blockHashesIn string, succ *bool) error

	// Check if certain blocks are alredy present on the server
	HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error
}
