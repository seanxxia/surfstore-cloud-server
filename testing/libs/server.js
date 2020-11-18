const fs = require('fs');
const path = require('path');
const tmp = require('tmp');
const shell = require('shelljs');
const kill = require('kill-port');
const mapFiles = require('map-files');

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
      async: true,
    });

  const cleanup = () => {
    shell.rm('-rf', path.join(dir.name, '*'));
    dir.removeCallback();
  };

  const writeFiles = (files) => writeFilesToDir(dir.name, files ?? {});
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

  return { run, writeFiles, readFiles, cleanup };
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
      if (content instanceof Buffer || typeof content === 'string') {
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
