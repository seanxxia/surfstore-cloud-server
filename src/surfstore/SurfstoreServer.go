package surfstore

import (
	"errors"
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
	// panic("todo")
	if *succ {
		e := s.MetaStore.GetFileInfoMap(succ, serverFileInfoMap)
		if e != nil {
			errors.New("Cannot get map")
		}
		return nil
	} else {
		return errors.New("Cannot find file")
	}
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	// panic("todo")
	e := s.MetaStore.UpdateFile(fileMetaData, latestVersion)
	if e != nil {
		errors.New("Cannot update")
	}
	return nil
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	// panic("todo")
	e := s.BlockStore.GetBlock(blockHash, blockData)
	if e != nil {
		errors.New("Cannot get block")
	}
	return nil
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {
	// panic("todo")
	e := s.BlockStore.PutBlock(blockData, succ)
	if e != nil {
		errors.New("Cannot put block")
	}
	return nil
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// panic("todo")
	e := s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
	if e != nil {
		errors.New("Cannot get blocks")
	}
	return nil
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
	// panic("todo")

	rpc.Register(&surfstoreServer)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", hostAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l, nil)

	// fmt.Println("Press enter key to end server")
	// fmt.Scanln()
	for {

	}

	return nil
}
