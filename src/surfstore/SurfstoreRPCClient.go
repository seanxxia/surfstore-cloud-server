package surfstore

import (
	"log"
	"net/rpc"
)

type RPCClient struct {
	ServerAddr string
	BaseDir    string
	BlockSize  int
	conn       *rpc.Client
}

func (surfClient *RPCClient) GetBlock(blockHash string, block *Block) error {
	err := surfClient.conn.Call("Server.GetBlock", blockHash, block)
	if err != nil {
		log.Println("Client::GetBlock - Failed to get block ", blockHash, err)
		return err
	}

	// log.Println("Client::GetBlock - Block received", blockHash)
	return nil
}

func (surfClient *RPCClient) HasBlock(blockHash string, succ *bool) error {
	err := surfClient.conn.Call("Server.HasBlock", blockHash, succ)
	if err != nil {
		log.Println("Client::HasBlock - Failed to check if server has block", blockHash, err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	err := surfClient.conn.Call("Server.PutBlock", block, succ)
	if err != nil {
		log.Println("Client::PutBlock - Failed to put block", block.Hash(), err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// perform the RPC call
	err := surfClient.conn.Call("Server.HasBlocks", blockHashesIn, blockHashesOut)
	if err != nil {
		log.Println("Client::HasBlocks - Failed to check if server has blocks", err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// perform the call
	err := surfClient.conn.Call("Server.GetFileInfoMap", succ, serverFileInfoMap)
	if err != nil {
		log.Println("Client::GetFileInfoMap - Failed to get file info map", err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMeta *FileMetaData, latestVersion *int) error {
	// perform the call
	err := surfClient.conn.Call("Server.UpdateFile", fileMeta, latestVersion)
	if err != nil {
		log.Println("Client::UpdateFile - Failed to update file meta:", fileMeta.Filename, err)
		return err
	}

	// close the connection
	return nil
}

func (surfClient *RPCClient) Close() error {
	return surfClient.conn.Close()
}

var _ Surfstore = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) (RPCClient, error) {

	client := RPCClient{
		ServerAddr: hostPort,
		BaseDir:    baseDir,
		BlockSize:  blockSize,
	}

	conn, err := rpc.DialHTTP("tcp", client.ServerAddr)
	if err != nil {
		log.Println("Failed to connect to server", err)
		return client, err
	}
	client.conn = conn
	return client, nil
}
