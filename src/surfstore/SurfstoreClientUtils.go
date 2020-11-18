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
	idxMetaMap := readIndexFile(client)

	// =============================create map for local dir======================
	idxMetaMap = updateFileMetaMapWithLocalFiles(client, idxMetaMap)

	// ============================ Now idxMetaMap is updated; try to compare with server map ===============
	// the idea is : not modify the server then download to local
	downloadblockmap := make(map[string]Block)
	// get server map
	serverfilemap := make(map[string]FileMetaData)
	var succ = true
	gmerr := client.GetFileInfoMap(&succ, &serverfilemap)
	if gmerr != nil {
		errors.New("get map error")
		log.Fatal("Cannot get map")
	}

	// working on existing files in server and local
	for remotename, remotemeta := range serverfilemap {
		if localmeta, ok := idxMetaMap[remotename]; ok { // if server match local file
			localversion := localmeta.Version
			remoteversion := remotemeta.Version
			localblist := localmeta.BlockHashList
			del := (len(localblist) == 1) && (localblist[0] == "0")
			if localversion > remoteversion { // modify and upload newest file to server

				if !del { //upload file
					uploadfile(client, remotename, &idxMetaMap)
				} else { // delete file from local
					var tombmeta FileMetaData
					tombmeta.Filename = remotename
					tombmeta.Version = localversion
					tombmeta.BlockHashList = []string{"0"}
					var v = new(int)
					client.UpdateFile(&tombmeta, v)
				}

			} else { // the version on the server is bigger -> download file to local
				// it may be a delete file
				remoteblist := remotemeta.BlockHashList
				tomb := (len(remoteblist) == 1) && (remoteblist[0] == "0")
				if !tomb {
					// download and update local map
					blocks := make([]Block, 0, len(remoteblist))
					DownloadnUpdate(client, &downloadblockmap, &remotemeta, &idxMetaMap, &blocks)
					// write
					writeFile(client, remotemeta, blocks)
				} else { // tomb file on server match local file: update the local map and delete the file from local
					//update
					var delmeta FileMetaData
					delmeta.Filename = remotemeta.Filename
					delmeta.Version = remotemeta.Version
					delmeta.BlockHashList = remotemeta.BlockHashList
					idxMetaMap[remotemeta.Filename] = &delmeta
					os.Remove(filepath.Join(client.BaseDir, remotemeta.Filename))
				}
			}
		} else { // server file not find in local.
			remoteblist := remotemeta.BlockHashList
			tomb := (len(remoteblist) == 1) && (remoteblist[0] == "0")
			if !tomb { //If not tomb then download and update to local
				// download and update
				blocks := make([]Block, 0, len(remoteblist))
				DownloadnUpdate(client, &downloadblockmap, &remotemeta, &idxMetaMap, &blocks)
				// write
				writeFile(client, remotemeta, blocks)
			} else { //If tomb then only update to local
				var delmeta FileMetaData
				delmeta.Filename = remotemeta.Filename
				delmeta.Version = remotemeta.Version
				delmeta.BlockHashList = remotemeta.BlockHashList
				idxMetaMap[remotemeta.Filename] = &delmeta
				os.Remove(filepath.Join(client.BaseDir, remotemeta.Filename))
			}

		}

	}
	//working on files only on local -> upload
	for localname, _ := range idxMetaMap {
		if _, ok := serverfilemap[localname]; !ok {
			uploadfile(client, localname, &idxMetaMap)
		}
	}

	newserverfilemap := new(map[string]FileMetaData)
	var s = true
	gmerr = client.GetFileInfoMap(&s, newserverfilemap)
	if gmerr != nil {
		errors.New("get map error")
	}

	// ==================================Finally, Write into a index file=============================
	writeindexFile(client, &idxMetaMap)
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

func uploadfile(client RPCClient, localname string, idxMetaMap *(map[string]*FileMetaData)) {
	// divide into blocks
	file, err := os.Open(filepath.Join(client.BaseDir, localname))
	if err != nil {
		errors.New("Cannot open local file")
		log.Fatal("Cannot open local file")
	}
	fileChunk := uint64(client.BlockSize)
	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	var blocklist []string

	// for empty file
	if totalPartsNum == 0 {
		partBuffer := make([]byte, 0)
		hash := sha256.Sum256(partBuffer)
		str := hex.EncodeToString(hash[:])
		blocklist = append(blocklist, str)
		//put block
		var block Block
		block.BlockData = partBuffer
		block.BlockSize = 0
		var succ = new(bool)
		pterr := client.PutBlock(block, succ)
		if *succ == false || pterr != nil {
			log.Println("Cannot put empty block to server")
		}
	}

	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(float64(fileChunk), float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)
		// write to hash
		hash := sha256.Sum256(partBuffer)
		str := hex.EncodeToString(hash[:])
		blocklist = append(blocklist, str)
		// write block to server

		var succ = new(bool)
		client.HasBlock(str, succ)
		//if there is error -> get block fail -> put block
		// if the error is nil -> get block succ -> no need
		if *succ { // found block
			fmt.Println("found block and not need to upload")
			continue
		}
		//put block
		var block Block
		block.BlockData = partBuffer
		block.BlockSize = partSize
		pterr := client.PutBlock(block, succ)
		if *succ == false || pterr != nil {
			log.Println("Cannot put block to server")
		}
	}

	// update block list to filemeta
	var meta FileMetaData
	meta.Filename = localname
	meta.Version = (*idxMetaMap)[localname].Version
	meta.BlockHashList = blocklist
	var v = new(int)
	uperr := client.UpdateFile(&meta, v)
	if uperr != nil {
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
		if localFileBlockHashList, ok := localFileMap[filename]; ok {
			// find the existing file
			if len(localFileBlockHashList) != len(fileMeta.BlockHashList) {
				fileMeta.BlockHashList = localFileBlockHashList
				fileMeta.Version++
			} else {
				isFileUpdated := false
				for i, blockHash := range localFileBlockHashList {
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
	for filename, localFileBlockHashList := range localFileMap {
		if _, ok := fileMetaMap[filename]; !ok {
			fileMeta := FileMetaData{
				Filename:      filename,
				Version:       1,
				BlockHashList: localFileBlockHashList,
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

func writeFile(client RPCClient, meta FileMetaData, blocks []Block) {
	file, err := os.OpenFile(filepath.Join(client.BaseDir, meta.Filename), os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		log.Println("file:" + meta.Filename + "cant not create and open")
	}
	defer file.Close()
	for _, b := range blocks {
		_, err := file.Write(b.BlockData)
		if err != nil {
			log.Println("file:" + meta.Filename + "write error")
			file.Close()
			return
		}
	}
	file.Sync()
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

func DownloadnUpdate(client RPCClient, downloadblockmap *map[string]Block, remotemeta *FileMetaData, idxMetaMap *map[string](*FileMetaData), blocks *[]Block) {
	// download
	// blocks := make([]Block, 0, len(remoteblist))
	for _, sha256 := range remotemeta.BlockHashList {
		var block Block
		block, ok := (*downloadblockmap)[sha256]
		if !ok {
			client.GetBlock(sha256, &block)
			(*downloadblockmap)[sha256] = block
		}
		*blocks = append(*blocks, block)
	}
	//update
	var newmeta FileMetaData
	newmeta.Filename = remotemeta.Filename
	newmeta.Version = remotemeta.Version
	newmeta.BlockHashList = remotemeta.BlockHashList
	(*idxMetaMap)[remotemeta.Filename] = &newmeta
}
