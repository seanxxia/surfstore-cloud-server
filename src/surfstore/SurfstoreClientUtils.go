package surfstore

import (
	"bufio"
	"fmt"
	"io"
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
	// ================================== create a map for old index.txt===============================
	fileMetaMap := readIndexFile(client)

	// =============================create map for local dir======================
	fileMetaMap = updateFileMetaMapWithLocalFiles(client, fileMetaMap)
	// PrintMetaMap(fileMetaMap)

	// ============================ Now idxMetaMap is updated; try to compare with server map ===============
	dummyRPCParam := true

	// the idea is : if cannot update then download
	retryMax := 3
	for i := 0; i < retryMax; i++ {
		// get server map
		remoteFileMetaMap := make(map[string]FileMetaData)
		err := client.GetFileInfoMap(&dummyRPCParam, &remoteFileMetaMap)
		if err != nil {
			log.Println("Failed to get remote file meta map", err)
			continue
		}

		isUploadFailed := false

		// working on existing files in server and local
		for remoteFilename, remoteFileMeta := range remoteFileMetaMap {
			// if server match local file
			if localFileMeta, ok := fileMetaMap[remoteFilename]; ok {
				// modify and upload newest file to server
				if localFileMeta.Version > remoteFileMeta.Version {
					isUploadFailed = isUploadFailed || !uploadFile(client, localFileMeta)
				} else {
					downloadFileAndUpdateLocalFileMeta(client, localFileMeta, &remoteFileMeta)
				}
			} else {
				// server file not find in local.
				var localFileMeta FileMetaData
				downloadFileAndUpdateLocalFileMeta(client, &localFileMeta, &remoteFileMeta)
				fileMetaMap[remoteFilename] = &localFileMeta
			}
		}

		// working on files only on local -> upload
		for localFilename, localFileMeta := range fileMetaMap {
			if _, ok := remoteFileMetaMap[localFilename]; !ok {
				isUploadFailed = isUploadFailed || !uploadFile(client, localFileMeta)
			}
		}

		if !isUploadFailed {
			break
		}
	}
	// ==================================Finally, Write into a index file=============================
	writeIndexFile(client, fileMetaMap)
}

func uploadFile(client RPCClient, fileMeta *FileMetaData) bool {
	// divide into blocks
	filename := fileMeta.Filename

	if fileMeta.IsTombstone() {
		var latestVersion int
		client.UpdateFile(fileMeta, &latestVersion)

		return fileMeta.Version == latestVersion
	}

	file, err := os.Open(filepath.Join(client.BaseDir, filename))
	if err != nil {
		log.Println("uploadFile: Failed to open file", filename, err)
		return false
	}

	blockSize := uint64(client.BlockSize)
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	numBlocks := uint64(math.Ceil(float64(fileSize) / float64(blockSize)))
	if numBlocks == 0 {
		// for empty file
		blockBuffer := make([]byte, 0)
		block := Block{Data: blockBuffer}

		succ := false
		err := client.PutBlock(block, &succ)
		if succ == false || err != nil {
			log.Println("uploadFile: Failed to put empty block to the server")
			return false
		}
	} else {
		for i := uint64(0); i < numBlocks; i++ {
			currentBlockSize := int(math.Min(float64(blockSize), float64(fileSize-int64(i*blockSize))))
			block := NewBlock(currentBlockSize)
			file.Read(block.Data)

			// write block to server
			// if there is error -> get block fail -> put block
			// if the error is nil -> get block succ -> no need
			succ := false
			client.HasBlock(block.Hash(), &succ)
			if !succ {
				succ := false
				err := client.PutBlock(block, &succ)
				if succ == false || err != nil {
					log.Println("uploadFile: Failed to put block to server")
					return false
				}
			}
		}
	}

	latestVersion := -1
	err = client.UpdateFile(fileMeta, &latestVersion)
	// TODO: Handle the case when latestVersion != fileMeta.Version
	if err != nil {
		return false
	}

	return fileMeta.Version == latestVersion
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
	reader := bufio.NewReader(indexFile)
	isReaderEnded := false
	for !isReaderEnded {
		line, err := reader.ReadString('\n')
		isReaderEnded = err == io.EOF
		if err != nil && err != io.EOF {
			panic(err)
		}
		if line == "" {
			break
		}

		text := strings.TrimSuffix(line, "\n")
		lineParts := strings.Split(text, ",")
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
			// if file is not mark as deleted in file meta, update it
			if !fileMeta.IsTombstone() {
				fileMeta.MarkTombstone()
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
			block := NewBlock(0)
			blockHashList = append(blockHashList, block.Hash())
		}

		for i := uint64(0); i < numBlocks; i++ {
			currentBlockSize := int(math.Min(float64(blockSize), float64(fileSize-int64(i*blockSize))))
			block := NewBlock(currentBlockSize)

			file.Read(block.Data)
			blockHashList = append(blockHashList, block.Hash())
		}
		localFileMap[fileInfo.Name()] = blockHashList
	}

	return localFileMap
}

func writeIndexFile(client RPCClient, fileMetaMap map[string]*FileMetaData) {
	// err := os.Truncate(filepath.Join(client.BaseDir, "index.txt"), 0)

	file, err := os.OpenFile(filepath.Join(client.BaseDir, "index.txt"), os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		panic(err)
	}

	for _, fileMeta := range fileMetaMap {
		line := fmt.Sprintf(
			"%s,%d,%s",
			fileMeta.Filename,
			fileMeta.Version,
			strings.Join(fileMeta.BlockHashList, " "),
		)
		line = strings.TrimSpace(line)

		_, err := file.WriteString(line + "\n")
		if err != nil {
			panic(err)
		}
	}
	file.Sync()
}

func downloadFileAndUpdateLocalFileMeta(client RPCClient, localFileMeta *FileMetaData, remoteFileMeta *FileMetaData) {
	// Get block map for remote
	var BlockMap map[string]Block
	for _, hash := range remoteFileMeta.BlockHashList {
		var nilBlock Block
		BlockMap[hash] = nilBlock
	}
	// update map with local blocks
	file, err := os.Open(filepath.Join(client.BaseDir, localFileMeta.Filename))
	if err != nil {
		panic(err)
	}
	for i, hash := range localFileMeta.BlockHashList {
		_, found := BlockMap[hash]
		// update correnspoding block from local
		if found && BlockMap[hash].Data == nil {
			currentBlockSize := client.BlockSize
			block := NewBlock(currentBlockSize)
			// buffer := make([]byte, client.BlockSize)
			_, err := file.ReadAt(block.Data, int64(i))
			if err != nil {
				panic(err)
			}
			BlockMap[hash] = block
		}

	}

	var fileBlocks []Block
	if !remoteFileMeta.IsTombstone() {
		for _, blockHash := range remoteFileMeta.BlockHashList {
			if BlockMap[blockHash].Data != nil {
				localblock := BlockMap[blockHash]
				fileBlocks = append(fileBlocks, localblock)
			} else {
				var block Block
				client.GetBlock(blockHash, &block)
				fileBlocks = append(fileBlocks, block)
			}

		}
	}
	localFileMeta.Filename = remoteFileMeta.Filename
	localFileMeta.Version = remoteFileMeta.Version
	localFileMeta.BlockHashList = remoteFileMeta.BlockHashList
	writeFile(client, localFileMeta, &fileBlocks)
}

func writeFile(client RPCClient, fileMeta *FileMetaData, blocks *[]Block) error {
	if fileMeta.IsTombstone() {
		os.Remove(filepath.Join(client.BaseDir, fileMeta.Filename))
	} else {
		file, err := os.Create(filepath.Join(client.BaseDir, fileMeta.Filename))
		if err != nil {
			log.Println("writeFile: Failed to open file:", fileMeta.Filename, err)
			return err
		}

		defer file.Close()
		for _, block := range *blocks {
			_, err := file.Write(block.Data)
			if err != nil {
				log.Println("writeFile: Failed to write to file:", fileMeta.Filename, err)
				return err
			}
		}
		file.Sync()
	}

	return nil
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
