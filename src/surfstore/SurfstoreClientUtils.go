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
	"strconv"
	"strings"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	// panic("todo")

	// ================================== create a map for old index.txt===============================
	idxMetaMap := make(map[string]*FileMetaData)

	idx_file, err := os.Open(client.BaseDir + "/index.txt") // For read access.
	if err != nil {                                         //not exist index
		idx_file, _ = os.Create(client.BaseDir + "/index.txt")
	}
	defer idx_file.Close()

	//read index file
	scanner := bufio.NewScanner(idx_file)
	for scanner.Scan() {
		// fmt.Println(scanner.Text())
		s := strings.Split(scanner.Text(), ",")
		if len(s) > 1 {

			filename := s[0]
			version, _ := strconv.Atoi(s[1])
			blist := s[2]
			bs := strings.Split(blist, " ")

			var meta FileMetaData
			meta.Filename = filename
			meta.Version = version
			meta.BlockHashList = bs
			idxMetaMap[filename] = &meta
		}

	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// =============================create map for local dir======================

	localfiles := make(map[string][]string)
	// open directory
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		errors.New("cannot read dir")
	}
	// iterate over all the files
	for _, f := range files {
		// fmt.Println(f.Name())
		if f.Name() == "index.txt" {
			continue
		}
		//check if files modified
		//version++
		file, err := os.Open(client.BaseDir + "/" + f.Name())
		if err != nil {
			errors.New("cannot read file")
		}
		// divide into blocks
		fileChunk := uint64(client.BlockSize)
		fileInfo, _ := file.Stat()
		var fileSize int64 = fileInfo.Size()
		totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

		var blocks []string
		for i := uint64(0); i < totalPartsNum; i++ {

			partSize := int(math.Min(float64(fileChunk), float64(fileSize-int64(i*fileChunk))))
			partBuffer := make([]byte, partSize)

			file.Read(partBuffer)
			// write to hash
			hash := sha256.Sum256(partBuffer)
			str := hex.EncodeToString(hash[:])
			// fmt.Println(str)
			blocks = append(blocks, str)
		}
		// fmt.Print(blocks)
		localfiles[f.Name()] = blocks
	}
	//iterate the old index map see if old file exists
	for key, idxmeta := range idxMetaMap {
		if localb, ok := localfiles[key]; ok { //find the existing file
			if len(localb) != len((*idxmeta).BlockHashList) {
				idxMetaMap[key].BlockHashList = localb
				idxMetaMap[key].Version = idxMetaMap[key].Version + 1
				continue
			}
			oriversion := (*idxmeta).Version
			for i, b := range localb {
				if b != idxmeta.BlockHashList[i] {
					idxMetaMap[key].BlockHashList[i] = b
					idxMetaMap[key].Version = oriversion + 1
				}
			}
			continue
		}
		// not find file in dir then delete
		tomb := []string{"0"}
		idxMetaMap[key].Version = idxMetaMap[key].Version + 1
		idxMetaMap[key].BlockHashList = tomb
	}

	//iterate new files to add new
	for name, blocks := range localfiles {
		if _, ok := idxMetaMap[name]; !ok {
			var meta FileMetaData
			meta.Filename = name
			meta.Version = 1
			meta.BlockHashList = blocks
			idxMetaMap[name] = &meta
		}
	}
	fmt.Println(idxMetaMap)
	// ============================ Now idxMetaMap is updated; try to compare with server map===============
	// the idea is : not modify the server then download to local
	// get server map
	fmt.Println("Start getting Server info")
	serverfilemap := make(map[string]FileMetaData)
	var succ = true
	gmerr := client.GetFileInfoMap(&succ, &serverfilemap)
	if gmerr != nil {
		errors.New("get map error")
		log.Fatal("Cannot get map")
	}
	fmt.Println("get the first server map:", serverfilemap)
	// working on existing files in server and local
	for remotename, remotemeta := range serverfilemap {
		if localmeta, ok := idxMetaMap[remotename]; ok { // if server match local file
			localversion := localmeta.Version
			remoteversion := remotemeta.Version
			if localversion > remoteversion { // modify and upload newest file to server

				// it may be a delete file
			} else { // download file to local
				// it may be a delete file
			}
			continue
		}
		// server not find in local, and check the tomb and download to client
	}

	//working on files only on local -> upload
	for localname, localmeta := range idxMetaMap {
		if _, ok := serverfilemap[localname]; !ok {
			// divide into blocks
			file, err := os.Open(client.BaseDir + "/" + localname)
			if err != nil {
				errors.New("Cannot open local file")
			}
			fileChunk := uint64(client.BlockSize)
			fileInfo, _ := file.Stat()
			var fileSize int64 = fileInfo.Size()
			totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

			var blocklist []string
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
				var block Block
				block.BlockData = partBuffer
				block.BlockSize = partSize
				pterr := client.PutBlock(block, succ)
				if *succ == false || pterr != nil {
					errors.New("Cannot put block to server")
					log.Fatal("Cannot put block to server")
				}
			}
			// update block list
			var meta FileMetaData
			meta.Filename = localname
			meta.Version = localmeta.Version
			meta.BlockHashList = blocklist
			var v = new(int)
			uperr := client.UpdateFile(&meta, v)
			if !(uperr != nil) {
				errors.New("Cannot update to server")
			}
		}
	}

	newserverfilemap := new(map[string]FileMetaData)
	var s = true
	gmerr = client.GetFileInfoMap(&s, newserverfilemap)
	if gmerr != nil {
		errors.New("get map error")
	}
	fmt.Println(*newserverfilemap)

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
