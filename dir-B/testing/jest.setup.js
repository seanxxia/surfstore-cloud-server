const fs = require('fs');
const path = require('path');
const { areBuffersEqual } = require('./libs/utils');

jest.setTimeout(200000);

expect.extend({
  toEqualBuffer(received, expected) {
    const pass = areBuffersEqual(received, expected);

    if (pass) {
      return {
        message: () =>
          `Expected: not ${this.utils.printExpected(expected)}\n` + `Received: ${this.utils.printReceived(received)}`,
        pass: true,
      };
    } else {
      return {
        message: () =>
          `Expected: ${this.utils.printExpected(expected)}\n` + `Received: ${this.utils.printReceived(received)}`,
        pass: false,
      };
    }
  },

  toHaveExactLocalFiles(receivedClient, filesMap) {
    const files = buildFilesFromFileMap(filesMap ?? {}, []);

    const clientFiles = receivedClient.readFiles();
    // ignore index.txt in client local files if exists
    delete clientFiles['index.txt'];

    const isFileNamesMatch =
      Object.keys(files).length === Object.keys(clientFiles).length &&
      Object.keys(files).every((fileName) => !!clientFiles[fileName]);

    if (!isFileNamesMatch) {
      return {
        message: () =>
          `Expected file names: ${this.utils.printExpected(Object.keys(files).sort())}\n` +
          `Received file names: ${this.utils.printReceived(Object.keys(clientFiles).sort())}`,
        pass: false,
      };
    }

    for (const [fileName, content] of Object.entries(files)) {
      if (!areBuffersEqual(content, clientFiles[fileName].contents)) {
        return {
          message: () => `File ${fileName} does not have the same content`,
          pass: false,
        };
      }
    }

    return {
      message: () => 'Client has exact expected files in its local storage',
      pass: true,
    };
  },

  toHaveExactSameLocalFilesAsClient(receivedClient, expectedClient) {
    const rcFiles = receivedClient.readFiles();
    const ecFiles = expectedClient.readFiles();

    // ignore index.txt in clients' local files if exists
    delete rcFiles['index.txt'];
    delete ecFiles['index.txt'];

    const isFileNamesMatch =
      Object.keys(rcFiles).length === Object.keys(ecFiles).length &&
      Object.keys(rcFiles).every((fileName) => !!ecFiles[fileName]);

    if (!isFileNamesMatch) {
      return {
        message: () =>
          `Expected cliet file names: ${this.utils.printExpected(Object.keys(ecFiles).sort())}\n` +
          `Received client file names: ${this.utils.printReceived(Object.keys(rcFiles).sort())}`,
        pass: false,
      };
    }

    for (const [fileName, content] of Object.entries(rcFiles)) {
      if (!areBuffersEqual(content, ecFiles[fileName].contents)) {
        return {
          message: () => `File ${fileName} does not have the same content`,
          pass: false,
        };
      }
    }

    return {
      message: () => 'Clients have exact same files in their local storages',
      pass: true,
    };
  },

  toHaveIndexFileHashesMatchLocalFileHashes(receivedClient, deletedFiles) {
    const message = receivedClient.isIndexFileHashesMatchLocalFileHashes();
    const pass = message === null;

    if (pass) {
      const index = receivedClient.readIndexFile();
      const tombstoneRecords = new Set();
      for (const [fileName, { hashList }] of Object.entries(index)) {
        if (hashList.length === 1 && hashList[0] === '0') {
          tombstoneRecords.add(fileName);
        }
      }
      for (const file of deletedFiles ?? []) {
        if (!tombstoneRecords.has(file)) {
          return {
            message: () => `${file} is not recorded deleted in the index file`,
            pass: false,
          };
        }
        tombstoneRecords.delete(file);
      }

      if (tombstoneRecords.length > 0) {
        return {
          message: () => `Unexpected tombstone records: ${[...tombstoneRecords]}`,
          pass: false,
        };
      }

      return {
        message: () => 'Client has index file with hashes math local file hashes',
        pass: true,
      };
    } else {
      return {
        message: () => message,
        pass: false,
      };
    }
  },

  toHaveIndexFileVersions(receivedClient, expectedFileVersions) {
    const clientIndex = receivedClient.readIndexFile();

    for (const [fileName, expectedVersion] of Object.entries(expectedFileVersions)) {
      if (!clientIndex[fileName]) {
        return {
          message: () => `File ${fileName} does not exist in client's index.txt`,
          pass: false,
        };
      }

      if (clientIndex[fileName].version !== expectedVersion) {
        return {
          message: () =>
            `File version mismatch ${fileName}\n` +
            `Expected file version: ${this.utils.printExpected(expectedVersion)}\n` +
            `Received file version: ${this.utils.printReceived(clientIndex[fileName].version)}`,
          pass: false,
        };
      }
    }

    return {
      message: () => `Client's index.txt has expected file versions`,
      pass: true,
    };
  },
});

function buildFilesFromFileMap(docs, prefix) {
  const files = {};
  for (const [name, content] of Object.entries(docs)) {
    if (content instanceof Function || typeof content === 'function') {
      let f = Buffer.alloc(0);
      const writeFunc = (content) => {
        if (content instanceof Buffer) {
          f = Buffer.concat([f, content]);
        } else {
          f = Buffer.concat([f, Buffer.from(content)]);
        }
      };
      const copyFunc = (src) => {
        f = fs.readFileSync(path.join(__dirname, './fixture', src));
      };
      content({ write: writeFunc, copy: copyFunc });
      files[name] = f;
    } else if (content instanceof Buffer) {
      files[name] = content;
    } else if (typeof content === 'string') {
      files[name] = Buffer.from(content);
    } else {
      const subdirFiles = buildFilesFromFileMap(content, [...prefix, name]);
      for (const [subdirFileName, file] of Object.entries(subdirFiles)) {
        files[path.join(name, subdirFileName)] = file;
      }
    }
  }
  return files;
}
