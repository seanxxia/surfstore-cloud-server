package surfstore

import (
	"fmt"
	"net/rpc"
)

type RPCClient struct {
	ServerAddr string
	BaseDir    string
	BlockSize  int
}

func (surfClient *RPCClient) GetBlock(blockHash string, block *Block) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.GetBlock", blockHash, block)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	// ==============> panic("todo")
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		fmt.Println("Not connected")
		return e
	} else {
		fmt.Println("connected for put")
	}
	// perform the call
	e = conn.Call("Server.PutBlock", block, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// ==============> panic("todo")
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("Server.HasBlocks", blockHashesIn, blockHashesOut)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// ==============> panic("todo")
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		fmt.Println("Not connected")
		return e
	} else {
		fmt.Println("connected for get info")
	}
	// perform the call
	e = conn.Call("Server.GetFileInfoMap", succ, serverFileInfoMap)
	if e != nil {
		conn.Close()
		fmt.Println("fail to call rpc getfileinfomap ")
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	// ==============> panic("todo")
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		fmt.Println("Not connected")
		return e
	} else {
		fmt.Println("connected for update file")
	}
	// perform the call
	e = conn.Call("Server.UpdateFile", fileMetaData, latestVersion)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
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
