package surfstore

import (
	"fmt"
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
		*succ = false
	}
	return err
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	err := s.MetaStore.UpdateFile(fileMetaData, latestVersion)
	return err
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	err := s.BlockStore.GetBlock(blockHash, blockData)
	return err
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {
	err := s.BlockStore.PutBlock(blockData, succ)
	if err != nil {
		*succ = false
	}
	return err
}

func (s *Server) HasBlock(blockHash string, succ *bool) error {
	err := s.BlockStore.HasBlock(blockHash, succ)
	return err
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	err := s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
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
	err := rpc.Register(&surfstoreServer)
	if err != nil {
		panic(err)
	}
	rpc.HandleHTTP()
	ln, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return err
	}

	go func() {
		err := http.Serve(ln, nil)
		if err != nil {
			panic(err)
		}
	}()
	// for {
	// }

	// arith := new(server.Arith)
	// rpc.Register(arith)
	// rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":8080")
	// if e != nil {
	// 	log.Fatal("listen error:", e)
	// }
	// go http.Serve(l, nil)

	fmt.Print("Press enter key to end server")
	fmt.Scanln()
	return nil
}
