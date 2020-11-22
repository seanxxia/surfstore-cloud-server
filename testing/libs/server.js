const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const tmp = require('tmp');
const shell = require('shelljs');
const fkill = require('fkill');
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
  const getClient = (files, options) => {
    const client = createClient(blockSize, files, options ?? {});
    clients.push(client);
    return client;
  };

  const cleanup = async () => {
    await fkill(serverProcess.pid);
    await fkill(`:${testingConfig['server-port']}`);

    for (const client of clients) {
      client.cleanup();
    }
  };

  return { getClient, cleanup };
}
module.exports.runServer = runServer;

function createClient(blockSize, files, options) {
  const dir = createTempDir(files ?? {});
  const execCommand = testingConfig['run-client-cmd']
    .replace('{ip:port}', `localhost:${testingConfig['server-port']}`)
    .replace('{basedir}', dir.name)
    .replace('{blocksize}', blockSize);

  const { silent = true } = options;

  const run = () =>
    shell.exec(execCommand, {
      silent,
      cwd: path.join(__dirname, '../../'),
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
          silent,
          cwd: path.join(__dirname, '../../'),
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
        Object.defineProperty(fileMap[fname], 'contents', {
          get: () => fs.readFileSync(path.join(dir.name, fname)),
        });
      }
    }
    return fileMap;
  };

  const readIndexFile = () => {
    const indexFileName = path.join(dir.name, 'index.txt');
    const content = fs.readFileSync(indexFileName).toString();
    const lines = content.trim().split('\n');
    const fileMetas = lines
      .map((line) => {
        if (line === '') {
          console.log('Index file has empty line');
          return null;
        }
        const [fileName, version, hashList] = line.split(',');
        return {
          fileName,
          version: parseInt(version),
          hashList: hashList.split(' ').map((h) => h.trim()),
        };
      })
      .filter((v) => v)
      .reduce((prev, { fileName, version, hashList }) => {
        prev[fileName] = { fileName, version, hashList };
        return prev;
      }, {});

    return fileMetas;
  };

  const isIndexFileHashesMatchLocalFileHashes = () => {
    const files = readFiles();
    const index = Object.values(readIndexFile()).filter(
      (fileMeta) => !(fileMeta.hashList.length === 1 && fileMeta.hashList[0] === '0')
    );

    if (index.length !== Object.keys(files).length - 1) {
      return `Number of index file records does not equal to number of local files`;
    }

    for (const { fileName, hashList } of index) {
      if (!files[fileName]) {
        return `File ${fileName}: does not exist in local storage`;
      }

      const fileBuffer = files[fileName].contents;
      const fileBlocks = fileBuffer.length > 0 ? [] : [Buffer.alloc(0)];

      for (let i = 0; i < fileBuffer.length; i += blockSize) {
        const j = Math.min(i + blockSize, fileBuffer.length);
        fileBlocks.push(fileBuffer.slice(i, j));
      }

      if (fileBlocks.length != hashList.length) {
        return `File ${fileName}: hash list length does not equal to file blocks length`;
      }

      for (let i = 0; i < fileBlocks.length; i++) {
        const hash = crypto.createHash('sha256').update(fileBlocks[i]).digest('hex');
        if (hash != hashList[i]) {
          return `File ${fileName}: hash lists mismatch`;
        }
      }
    }
    return null;
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
    dir: dir.name,
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
