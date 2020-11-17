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
	idxMetaMap := make(map[string]*FileMetaData)

	idx_file, err := os.Open(filepath.Join(client.BaseDir, "index.txt")) // For read access.

	if err != nil { //not exist index
		idx_file, _ = os.Create(filepath.Join(client.BaseDir, "index.txt"))
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
		file, err := os.Open(filepath.Join(client.BaseDir, f.Name()))
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
	downloadblockmap := make(map[string]Block)
	// get server map
	fmt.Println("Start getting Server info")
	serverfilemap := make(map[string]FileMetaData)
	var succ = true
	gmerr := client.GetFileInfoMap(&succ, &serverfilemap)
	if gmerr != nil {
		errors.New("get map error")
		log.Fatal("Cannot get map")
	}
	fmt.Println("get the current server map:", serverfilemap)
	// working on existing files in server and local
	for remotename, remotemeta := range serverfilemap {
		fmt.Println("in server map:", remotename, remotemeta)
		if localmeta, ok := idxMetaMap[remotename]; ok { // if server match local file
			localversion := localmeta.Version
			remoteversion := remotemeta.Version
			localblist := localmeta.BlockHashList
			del := (len(localblist) == 1) && (localblist[0] == "0")
			if localversion > remoteversion { // modify and upload newest file to server

				if !del {
					uploadfile(client, remotename, &idxMetaMap)
				} else { // it may be a delete file
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
				}
			}
		} else { // server not find in local. If not tomb then download to local
			remoteblist := remotemeta.BlockHashList
			tomb := (len(remoteblist) == 1) && (remoteblist[0] == "0")
			if !tomb {
				// download and update
				blocks := make([]Block, 0, len(remoteblist))
				DownloadnUpdate(client, &downloadblockmap, &remotemeta, &idxMetaMap, &blocks)
				// write
				writeFile(client, remotemeta, blocks)
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
	fmt.Println(">>>>>>>>>>uploading:", localname)
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

	fmt.Println("start splitting file in ", totalPartsNum, " times")
	// for empty file
	if totalPartsNum == 0 {
		fmt.Println("empty file to sha256")
		partBuffer := make([]byte, 0)
		hash := sha256.Sum256(partBuffer)
		fmt.Println(hash)
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

func DownloadnUpdate(client RPCClient, downloadblockmap *map[string]Block, remotemeta *FileMetaData, idxMetaMap *map[string](*FileMetaData), blocks *[]Block) {
	// download
	// blocks := make([]Block, 0, len(remoteblist))
	for _, sha256 := range remotemeta.BlockHashList {
		var block Block
		block, ok := (*downloadblockmap)[sha256]
		fmt.Println("getting block from file:", remotemeta.Filename)
		if !ok {
			fmt.Println("finding block:", sha256)
			client.GetBlock(sha256, &block)
			fmt.Println("getting block:", block.BlockData)
			(*downloadblockmap)[sha256] = block
		}
		*blocks = append(*blocks, block)
	}
	//update
	// idxMetaMap := make(map[string]*FileMetaData)
	var newmeta FileMetaData
	newmeta.Filename = remotemeta.Filename
	newmeta.Version = remotemeta.Version
	newmeta.BlockHashList = remotemeta.BlockHashList
	(*idxMetaMap)[remotemeta.Filename] = &newmeta
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

		var line string
		line = n + "," + strconv.Itoa(v) + ","
		for _, hash := range blist {
			line = line + hash + " "
		}
		line = strings.TrimSpace(line)
		fmt.Println("final output:" + line)
		_, err := f.WriteString(line + "\n")
		if err != nil {
			log.Fatal(err)
		}

	}
	f.Sync()
}
