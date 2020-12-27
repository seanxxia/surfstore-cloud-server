# Surfstore Cloud Server
![Build & Test](
	https://github.com/summer110669/surfstore-cloud-server/workflows/Build%20&%20Test/badge.svg)
![Format & Lint](
    https://github.com/summer110669/surfstore-cloud-server/workflows/Lint%20&%20Format/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

<div align="center"><img width="300" src="./ucsd-logo.png" /></div>

An implementation cloud-based file storage service named SurfStore. SurfStore is a networked file storage application that is based on Dropbox, and lets you sync files to and from the “cloud”. 

## Usage
### Step 0: Install Go and Node.js

You need to [install Go](https://golang.org/doc/install) to build and run the web server.

The tests are written in JavaScript. To run the tests locally, you need to install [Node.js](https://nodejs.org/en/).


### Step 1: Set environment variables

If you have Node.js installed locally, there are npm scripts to build the project and run the test that sets the environment variables automatically. To use the npm scripts, you need to install the dependencies first with the command:
```
npm install
```

Otherwise, you need to add directory into ```.bash_profile``` (for OS X environment) or ```.bashrc``` to let the compiler knows where to find the dependencies
```
export PATH=$PATH:/usr/local/go/bin     # making sure go is on path
export GOPATH=<path-to-repo>
export PATH=$PATH:$GOPATH/bin
 ```
example:
```
export GOPATH=/[The directory you put this folder]/Project-1/
export PATH=$PATH:$GOPATH/bin
```

### Step 2: Build and run the server

```shell
./build.sh
./run-server.sh
```

### Step 3: Run clients

From a new terminal (or a new node), run the client using the script. 
Create folders to add or remove files within. Finally, run the script to sync with the server.

```shell
mkdir dataA
cp ~/pic.jpg dataA/ 
./run-client.sh server_addr:port dataA 4096
```

This would sync pic.jpg to the server hosted on `server_addr:port`, using
`dataA` as the base directory, with a block size of 4096 bytes.

From another terminal (or a new node), run the client to sync with the
server. (if using a new node, build using step 1 first)

```shell
ls dataB/
./run-client.sh server_addr:port dataB 4096
ls dataB/
pic.jpg index.txt
```

We should observe that pic.jpg has been synced to this client.

## Testing
To run the test, you need to install the Node.js test dependencies first with the following command:
```
npm install
```
The test is also available through npm script. With the following command, it will automatically build the code and run the test suites.
```
npm run test
```  
For more information of the testing, jump to [README.md inside the testing folder](testing/README.md).

## Project Structure

1. The SurfStore service is composed of two services: BlockStore and MetadataStore
2. A file in SurfStore is broken into an ordered sequence of one or more blocks which are stored in the BlockStore.
3. The MetadataStore maintains the mapping of filenames to hashes of these blocks (and versions) in a map.

### Surfstore Interface

`SurfstoreInterfaces.go` has structures that define the file block and metadata, and it has interfaces to retrieve metadata and upload files to the server.

### Server

`BlockStore.go` provides an implementation of the `BlockStoreInterface`, and `MetaStore.go` provides an implementation of the
`MetaStoreInterface`.

`SurfstoreServer.go` puts everything together to provide a complete implementation of the `Surfstore` interface and starts
listening for connections from clients.

### Client

`SurfstoreRPCClient.go` provides the rpc client stub for the surfstore rpc server.

`SurfstoreClientUtils.go` has utility functions.