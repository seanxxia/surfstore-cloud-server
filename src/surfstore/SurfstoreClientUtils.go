package surfstore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	// panic("todo")

	// ================================== create a map for old index.txt===============================
	fileMetaMap := readIndexFile(client)

	// =============================create map for local dir======================
	fileMetaMap = updateFileMetaMapWithLocalFiles(client, fileMetaMap)

	// ============================ Now idxMetaMap is updated; try to compare with server map ===============
	dummyRPCParam := true

	// the idea is : not modify the server then download to local
	// get server map
	remoteFileMetaMap := make(map[string]FileMetaData)

	err := client.GetFileInfoMap(&dummyRPCParam, &remoteFileMetaMap)
	if err != nil {
		log.Fatal("Failed to get remote file meta map")
		panic(err)
	}

	// working on existing files in server and local
	for remoteFilename, remoteFileMeta := range remoteFileMetaMap {
		if localFileMeta, ok := fileMetaMap[remoteFilename]; ok { // if server match local file
			if localFileMeta.Version > remoteFileMeta.Version { // modify and upload newest file to server
				uploadFile(client, localFileMeta)
			} else {
				downloadFile(client, localFileMeta, &remoteFileMeta)
			}
		} else {
			// server file not find in local.
			var localFileMeta FileMetaData
			downloadFile(client, &localFileMeta, &remoteFileMeta)
			fileMetaMap[remoteFilename] = &localFileMeta
		}
	}

	// working on files only on local -> upload
	for localFilename, localFileMeta := range fileMetaMap {
		if _, ok := remoteFileMetaMap[localFilename]; !ok {
			uploadFile(client, localFileMeta)
		}
	}

	newserverfilemap := new(map[string]FileMetaData)
	succ := false
	err = client.GetFileInfoMap(&succ, newserverfilemap)
	if err != nil {
		errors.New("get map error")
	}

	// ==================================Finally, Write into a index file=============================
	writeindexFile(client, &fileMetaMap)
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}

func uploadFile(client RPCClient, fileMeta *FileMetaData) {
	// divide into blocks
	filename := fileMeta.Filename

	if len(fileMeta.BlockHashList) == 1 && fileMeta.BlockHashList[0] == "0" {
		var v int
		client.UpdateFile(fileMeta, &v)

		return
	}

	file, err := os.Open(filepath.Join(client.BaseDir, filename))
	if err != nil {
		log.Fatal("Cannot open local file")
		panic(err)
	}

	blockSize := uint64(client.BlockSize)
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	numBlocks := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))
	var blockHashList []string

	// for empty file
	if numBlocks == 0 {
		blockBuffer := make([]byte, 0)
		blockHash := getBufferHash(&blockBuffer)
		blockHashList = append(blockHashList, blockHash)

		block := Block{
			BlockData: blockBuffer,
			BlockSize: 0,
		}

		succ := false
		err := client.PutBlock(block, &succ)
		if succ == false || err != nil {
			log.Println("Cannot put empty block to server")
		}
	} else {
		for i := uint64(0); i < numBlocks; i++ {
			currentBlockSize := int(math.Min(float64(blockSize), float64(fileSize-int64(i*blockSize))))
			blockBuffer := make([]byte, currentBlockSize)
			file.Read(blockBuffer)

			// write to hash
			blockHash := getBufferHash(&blockBuffer)
			blockHashList = append(blockHashList, blockHash)

			// write block to server
			succ := false
			client.HasBlock(blockHash, &succ)

			// if there is error -> get block fail -> put block
			// if the error is nil -> get block succ -> no need
			if succ { // found block
				fmt.Println("found block and not need to upload")
			} else {
				//put block
				block := Block{
					BlockData: blockBuffer,
					BlockSize: currentBlockSize,
				}

				succ := false
				err := client.PutBlock(block, &succ)
				if succ == false || err != nil {
					log.Println("Cannot put block to server")
				}
			}
		}
	}

	var latestVersion int
	err = client.UpdateFile(fileMeta, &latestVersion)
	// TODO: Handle the case when latestVersion != fileMeta.Version
	if err != nil {
		log.Fatal("Cannot update to server")
	}
}

func readIndexFile(client RPCClient) map[string]*FileMetaData {
	// For read access.
	indexFilename := filepath.Join(client.BaseDir, "index.txt")
	indexFile, err := os.Open(indexFilename)

	if err != nil {
		// index.txt does not exit
		indexFile, err = os.Create(indexFilename)
		if err != nil {
			panic(err)
		}
	}
	defer indexFile.Close()

	fileMetaMap := make(map[string]*FileMetaData)

	// read index file
	scanner := bufio.NewScanner(indexFile)
	for scanner.Scan() {
		// fmt.Println(scanner.Text())
		lineParts := strings.Split(scanner.Text(), ",")
		if len(lineParts) == 3 {
			filename := lineParts[0]
			version, _ := strconv.Atoi(lineParts[1])
			blockHasheListString := lineParts[2]
			blockHasheList := strings.Split(blockHasheListString, " ")

			fileMeta := FileMetaData{
				Filename:      filename,
				Version:       version,
				BlockHashList: blockHasheList,
			}
			fileMetaMap[filename] = &fileMeta
		} else {
			panic("Invalid index.txt")
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return fileMetaMap
}

func updateFileMetaMapWithLocalFiles(client RPCClient, fileMetaMap map[string]*FileMetaData) map[string]*FileMetaData {
	localFileMap := getLocalFileHashBlockListMap(client)

	// iterate over the file meta map and see if old file exists
	for filename, fileMeta := range fileMetaMap {
		if localBlockHashList, ok := localFileMap[filename]; ok {
			// find the existing file
			if len(localBlockHashList) != len(fileMeta.BlockHashList) {
				fileMeta.BlockHashList = localBlockHashList
				fileMeta.Version++
			} else {
				isFileUpdated := false
				for i, blockHash := range localBlockHashList {
					if blockHash != fileMeta.BlockHashList[i] {
						fileMeta.BlockHashList[i] = blockHash
						isFileUpdated = true
					}
				}

				if isFileUpdated {
					fileMeta.Version++
				}
			}
		} else {
			// file does not exist in dir, shoud be deleted
			isAlreadyDeleted := len(fileMeta.BlockHashList) == 1 && fileMeta.BlockHashList[0] == "0"

			// if file is not mark as deleted in file meta, update it
			if !isAlreadyDeleted {
				tombstoneBlockHashList := []string{"0"}
				fileMeta.BlockHashList = tombstoneBlockHashList
				fileMeta.Version++
			}
		}
	}

	// iterate over the local files and create new files
	for filename, localBlockHashList := range localFileMap {
		if _, ok := fileMetaMap[filename]; !ok {
			fileMeta := FileMetaData{
				Filename:      filename,
				Version:       1,
				BlockHashList: localBlockHashList,
			}
			fileMetaMap[filename] = &fileMeta
		}
	}

	fmt.Println(fileMetaMap)
	return fileMetaMap
}

func getLocalFileHashBlockListMap(client RPCClient) map[string][]string {
	// open directory
	localFileInfos, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		panic(err)
	}

	localFileMap := make(map[string][]string)
	// iterate over all the local files
	for _, fileInfo := range localFileInfos {
		if fileInfo.Name() == "index.txt" {
			continue
		}
		// check if the file is modified

		file, err := os.Open(filepath.Join(client.BaseDir, fileInfo.Name()))
		if err != nil {
			panic(err)
		}

		// divide into blocks
		fileSize := fileInfo.Size()
		blockSize := uint64(client.BlockSize)
		numBlocks := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))

		var blockHashList []string

		// for empty file
		if numBlocks == 0 {
			// write to hash
			blockBuffer := make([]byte, 0)
			hash := getBufferHash(&blockBuffer)
			blockHashList = append(blockHashList, hash)
		}

		for i := uint64(0); i < numBlocks; i++ {
			currentBlockSize := int(math.Min(float64(blockSize), float64(fileSize-int64(i*blockSize))))
			blockBuffer := make([]byte, currentBlockSize)

			file.Read(blockBuffer)
			hash := getBufferHash(&blockBuffer)
			blockHashList = append(blockHashList, hash)
		}
		localFileMap[fileInfo.Name()] = blockHashList
	}

	return localFileMap
}

func writeindexFile(client RPCClient, idxMetaMap *map[string](*FileMetaData)) {
	// err := os.Truncate(filepath.Join(client.BaseDir, "index.txt"), 0)

	f, err := os.OpenFile(filepath.Join(client.BaseDir, "index.txt"), os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}

	for _, meta := range *idxMetaMap {
		n := meta.Filename
		v := meta.Version
		blist := meta.BlockHashList
		fmt.Println("=======>", blist)

		var line string
		line = n + "," + strconv.Itoa(v) + ","
		for _, hash := range blist {
			line = line + hash + " "
		}
		line = strings.TrimSpace(line)
		_, err := f.WriteString(line + "\n")
		if err != nil {
			log.Fatal(err)
		}

	}
	f.Sync()
}

func getBufferHash(buffer *[]byte) string {
	hash := sha256.Sum256(*buffer)
	hashString := hex.EncodeToString(hash[:])
	return hashString
}

func downloadFile(client RPCClient, localFileMeta *FileMetaData, remoteFileMeta *FileMetaData) {
	var fileBlocks []Block

	if len(remoteFileMeta.BlockHashList) != 1 || remoteFileMeta.BlockHashList[0] != "0" {
		for _, blockHash := range remoteFileMeta.BlockHashList {
			var block Block
			client.GetBlock(blockHash, &block)
			fileBlocks = append(fileBlocks, block)
		}
	}

	localFileMeta.Filename = remoteFileMeta.Filename
	localFileMeta.Version = remoteFileMeta.Version
	localFileMeta.BlockHashList = remoteFileMeta.BlockHashList
	fmt.Println("QWWEWEWEWEWEEs")
	writeFile(client, localFileMeta, &fileBlocks)
}

func writeFile(client RPCClient, fileMeta *FileMetaData, blocks *[]Block) {
	if len(fileMeta.BlockHashList) == 0 && fileMeta.BlockHashList[0] == "0" {
		os.Remove(filepath.Join(client.BaseDir, fileMeta.Filename))
	} else {
		file, err := os.OpenFile(filepath.Join(client.BaseDir, fileMeta.Filename), os.O_CREATE|os.O_RDWR, 0755)
		if err != nil {
			log.Println("file:" + fileMeta.Filename + "cant not create and open")
		} else {
			defer file.Close()
			for _, block := range *blocks {
				_, err := file.Write(block.BlockData)
				if err != nil {
					log.Println("file:" + fileMeta.Filename + "write error")
					return
				}
			}
			file.Sync()
		}
	}
}
