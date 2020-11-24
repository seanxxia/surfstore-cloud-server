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
	err := s.MetaStore.GetFileInfoMap(succ, serverFileInfoMap)
	if err != nil {
		log.Println(err)
		*succ = false
	}
	return err
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	err := s.MetaStore.UpdateFile(fileMetaData, latestVersion)
	if err != nil {
		log.Println(err)
	}
	return err
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	err := s.BlockStore.GetBlock(blockHash, blockData)
	if err != nil {
		log.Println(err)
	}
	return err
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {
	err := s.BlockStore.PutBlock(blockData, succ)
	if err != nil {
		log.Println(err)
		*succ = false
	}
	return err
}

func (s *Server) HasBlock(blockHash string, succ *bool) error {
	err := s.BlockStore.HasBlock(blockHash, succ)
	if err != nil {
		log.Println(err)
	}
	return err
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	err := s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
	if err != nil {
		log.Println(err)
	}
	return err
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
	rpc.Register(&surfstoreServer)
	rpc.HandleHTTP()
	ln, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return err
	}

	http.Serve(ln, nil)

	for {
	}
}
