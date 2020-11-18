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
		log.Println("Client::GetBlock - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.GetBlock", blockHash, block)
	if err != nil {
		log.Println("Client::GetBlock - Failed to get block ", blockHash)
		return err
	}

	log.Println("Client::GetBlock - Block received", blockHash)
	return nil
}

func (surfClient *RPCClient) HasBlock(blockHash string, succ *bool) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Println("Client::HasBlock - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.HasBlock", blockHash, succ)
	if err != nil {
		log.Println("Client::HasBlock - Failed to check if server has block", blockHash)
		log.Println(err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Println("Client::PutBlock - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.PutBlock", block, succ)
	if err != nil {
		log.Println("Client::PutBlock - Failed to put block", block.Hash())
		return err
	}

	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Println("Client::HasBlocks - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the RPC call
	err = conn.Call("Server.HasBlocks", blockHashesIn, blockHashesOut)
	if err != nil {
		log.Println("Client::HasBlocks - Failed to check if server has blocks")
		return err
	}

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Println("Client::GetFileInfoMap - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the call
	err = conn.Call("Server.GetFileInfoMap", succ, serverFileInfoMap)
	if err != nil {
		log.Println("Client::GetFileInfoMap - Failed to get file info map")
		return err
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMeta *FileMetaData, latestVersion *int) error {
	// connect to the server
	conn, err := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if err != nil {
		log.Println("Client::UpdateFile - Failed to connect to server")
		return err
	}
	defer conn.Close()

	// perform the call
	err = conn.Call("Server.UpdateFile", fileMeta, latestVersion)
	if err != nil {
		log.Println("Client::UpdateFile - Failed to update file meta:", fileMeta.Filename)
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
