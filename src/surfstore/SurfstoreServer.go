package surfstore

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
}

func (s *Server) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	panic("todo")
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	panic("todo")
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	panic("todo")

func (s *Server) PutBlock(blockData Block, succ *bool) error {
	panic("todo")

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	panic("todo")
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}

	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
	}
}

func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {
	panic("todo")
}
