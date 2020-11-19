const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const tmp = require('tmp');
const shell = require('shelljs');
const kill = require('kill-port');
const mapFiles = require('map-files');
const { sleep } = require('./utils');

const { testing: testingConfig } = require('../../package.json');

function runServer(blockSize) {
  const execCommand = testingConfig['run-server-cmd'];

  const serverProcess = shell.exec(execCommand, {
    cwd: path.join(__dirname, '../../'),
    silent: true,
    async: true,
  });

  const clients = [];
  const getClient = (files) => {
    const client = createClient(blockSize, files);
    clients.push(client);
    return client;
  };

  const cleanup = async () => {
    serverProcess.kill('SIGKILL');
    await kill(testingConfig['server-port'], 'tcp');
    for (const client of clients) {
      client.cleanup();
    }
  };

  return { getClient, cleanup };
}
module.exports.runServer = runServer;

function createClient(blockSize, files) {
  const dir = createTempDir(files ?? {});
  const execCommand = testingConfig['run-client-cmd']
    .replace('{ip:port}', `localhost:${testingConfig['server-port']}`)
    .replace('{basedir}', dir.name)
    .replace('{blocksize}', blockSize);

  const run = () =>
    shell.exec(execCommand, {
      cwd: path.join(__dirname, '../../'),
      silent: true,
      async: false,
    });

  const runAsync = async (delayMiliSeconds = -1) => {
    if (delayMiliSeconds >= 0) {
      await sleep(delayMiliSeconds);
    }
    return await new Promise((resolve) => {
      shell.exec(
        execCommand,
        {
          cwd: path.join(__dirname, '../../'),
          silent: true,
          async: true,
        },
        () => {
          resolve();
        }
      );
    });
  };

  const cleanup = () => {
    shell.rm('-rf', path.join(dir.name, '*'));
    dir.removeCallback();
  };

  const writeFiles = (files) => writeFilesToDir(dir.name, files ?? {});
  const deleteFiles = (files) => {
    for (const file of files) {
      shell.rm('-rf', path.join(dir.name, file));
    }
  };

  const readFiles = () => {
    const fileDirMap = mapFiles(path.join(dir.name, './**/*'), { cwd: dir.name });
    const fileMap = {};
    for (const fname in fileDirMap) {
      if (fs.lstatSync(path.join(dir.name, fname)).isFile()) {
        fileMap[fname] = fileDirMap[fname];
      }
    }
    return fileMap;
  };

  const readIndexFile = () => {
    const indexFileName = path.join(dir.name, 'index.txt');
    const content = fs.readFileSync(indexFileName).toString();
    const lines = content.trim().split('\n');
    const fileMetas = lines.map((line) => {
      const [fileName, version, hashList] = line.split(',');
      return {
        fileName,
        version,
        hashList: hashList.split(' ').map((h) => h.trim()),
      };
    });

    return fileMetas;
  };

  const isIndexFileHashesMatchLocalFileHashes = () => {
    const index = readIndexFile();
    const files = readFiles();

    for (const fileName of Object.keys(index)) {
      if (index[fileName].hashList.length === 0 && index[file].hashList[0] === '0') {
        // tombstone record
        delete index[fileName];
      }
    }

    if (index.length !== Object.keys(files).length - 1) {
      console.log(`Number of index file records does not equal to number of local files`);
      return false;
    }

    for (const { fileName, hashList } of index) {
      if (!files[fileName]) {
        console.log(`File ${fileName}: does not exist in local storage`);
        return false;
      }

      const fileBuffer = files[fileName].contents;
      const fileBlocks = [];

      for (let i = 0; i < fileBuffer.length; i += blockSize) {
        const j = Math.min(i + blockSize, fileBuffer.length);
        fileBlocks.push(fileBuffer.slice(i, j));
      }

      if (fileBlocks.length != hashList.length) {
        console.log(fileBlocks.length, hashList.length);
        console.log(`File ${fileName}: hash list length does not equal to file blocks length`);
        return false;
      }

      for (const i in fileBlocks) {
        const hash = crypto.createHash('sha256').update(fileBlocks[i]).digest('hex');
        if (hash != hashList[i]) {
          console.log(`File ${fileName}: hash lists mismatch`);
          return false;
        }
      }
    }
    return true;
  };

  return {
    run,
    runAsync,
    writeFiles,
    readFiles,
    deleteFiles,
    cleanup,
    readIndexFile,
    isIndexFileHashesMatchLocalFileHashes,
  };
}

function createTempDir(files) {
  const dir = tmp.dirSync({
    prefix: 'surfstore-test-docs',
  });

  writeFilesToDir(dir.name, files ?? {});
  return dir;
}

function writeFilesToDir(dirname, files) {
  const getPath = (...prefix) => path.join(dirname, ...prefix);
  shell.mkdir('-p', getPath('./'));

  const build = (docs, prefix) => {
    for (const [name, content] of Object.entries(docs)) {
      if (content instanceof Function || typeof content === 'function') {
        const fileName = getPath(...prefix, name);

        let f = null;
        const writeFunc = (data) => {
          if (!f) {
            f = fs.openSync(fileName, 'w');
          }
          fs.writeSync(f, data);
        };
        const copyFunc = (src) => {
          if (f) {
            throw new Error('Cannot use copy and write function together');
          }
          fs.copyFileSync(path.join(__dirname, '../fixture', src), fileName);
        };
        content({ write: writeFunc, copy: copyFunc });
        if (f) {
          fs.closeSync(f);
        }
      } else if (content instanceof Buffer || typeof content === 'string') {
        const fileName = getPath(...prefix, name);
        fs.writeFileSync(fileName, content);
      } else {
        shell.mkdir('-p', getPath(...prefix, name));
        build(content, [...prefix, name]);
      }
    }
  };

  build(files ?? {}, []);
}
