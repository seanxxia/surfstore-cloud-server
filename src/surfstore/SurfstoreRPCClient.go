package surfstore

import (
	"log"
	"net/rpc"
)

type RPCClient struct {
	ServerAddr string
	BaseDir    string
	BlockSize  int
}

func (surfClient *RPCClient) GetBlock(blockHash string, block *Block) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::GetBlock - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.GetBlock", blockHash, block)
	if err != nil {
		log.Fatalln("Client::GetBlock - Failed to get block ", blockHash, err)
		return err
	}

	// log.Println("Client::GetBlock - Block received", blockHash)
	return nil
}

func (surfClient *RPCClient) HasBlock(blockHash string, succ *bool) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::HasBlock - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.HasBlock", blockHash, succ)
	if err != nil {
		log.Fatalln("Client::HasBlock - Failed to check if server has block", blockHash, err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::PutBlock - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.PutBlock", block, succ)
	if err != nil {
		log.Fatalln("Client::PutBlock - Failed to put block", block.Hash(), err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::HasBlocks - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.HasBlocks", blockHashesIn, blockHashesOut)
	if err != nil {
		log.Fatalln("Client::HasBlocks - Failed to check if server has blocks", err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::GetFileInfoMap - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the call
	err = conn.Call("Server.GetFileInfoMap", succ, serverFileInfoMap)
	if err != nil {
		log.Fatalln("Client::GetFileInfoMap - Failed to get file info map", err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMeta *FileMetaData, latestVersion *int) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Fatalln("Client::UpdateFile - Failed to connect to server", err)
		return err
	}
	defer conn.Close()

	// perform the call
	err = conn.Call("Server.UpdateFile", fileMeta, latestVersion)
	if err != nil {
		log.Fatalln("Client::UpdateFile - Failed to update file meta:", fileMeta.Filename, err)
		return err
	}

	// close the connection
	return nil
}

var _ Surfstore = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		ServerAddr: hostPort,
		BaseDir:    baseDir,
		BlockSize:  blockSize,
	}
}
