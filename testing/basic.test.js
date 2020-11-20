const fs = require('fs');
const path = require('path');
const { runServer } = require('./libs/server');
const { waitForServerStart, areBuffersEqual } = require('./libs/utils');

let server;
let getClient;

beforeEach(async () => {
  // blockSize = 4096
  server = runServer(4096);
  getClient = server.getClient;
  await waitForServerStart();
});

afterEach(async () => {
  await server.cleanup();
});

test('should sync files.', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 1,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync files (concurrent).', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();

  // Just a toy example
  await Promise.all([
    client1.runAsync(),
    client2.runAsync(500), // Delay starting the process after 500ms
  ]);

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 1,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync files (bytes).', async () => {
  const files = {
    'video.mp4': ({ copy }) => {
      // Copy from file in dir "testing/fixture"
      copy('video.mp4');
    },
    'large.txt': ({ write }) => {
      // Write string (buffer) to file
      for (let i = 0; i < 100; i++) {
        write('ABC');
      }
    },
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    'video.mp4': 1,
    'large.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync updates.', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 2,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync deletes.', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  delete files['t1.txt'];
  client1.deleteFiles(['t1.txt']);

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 2,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync empty files', async () => {
  const files = {
    't1.txt': '',
    't2.txt': 'This is test2',
  };
  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 1,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync update to empty files', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  files['t1.txt'] = '';
  client1.writeFiles({ 't1.txt': '' });

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 2,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync append/delete content from file', async () => {
  const files = {
    't1.txt': Buffer.alloc(1024 * 4, '1'),
    't2.txt': Buffer.alloc(1024 * 4, '2'),
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  // t1.txt append content
  files['t1.txt'] = Buffer.alloc(1024 * 8, '1');
  client1.writeFiles({ 't1.txt': files['t1.txt'] });

  // t2.txt delete content
  files['t2.txt'] = Buffer.alloc(1024 * 2, '2');
  client1.writeFiles({ 't2.txt': files['t2.txt'] });

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();

  const expectedFileVersions = {
    't1.txt': 2,
    't2.txt': 2,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync create delete recreate delete recreate.', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  delete files['t1.txt'];
  client1.deleteFiles(['t1.txt']);

  client1.run();
  client2.run();

  // c1 recreate
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });
  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client1).toHaveIndexFileVersions({
    't1.txt': 3,
    't2.txt': 1,
  });
  expect(client2).toHaveIndexFileVersions({
    't1.txt': 3,
    't2.txt': 1,
  });

  // c2 delete
  delete files['t1.txt'];
  client2.deleteFiles(['t1.txt']);

  client2.run();
  client1.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client1).toHaveIndexFileVersions({
    't1.txt': 4,
    't2.txt': 1,
  });
  expect(client2).toHaveIndexFileVersions({
    't1.txt': 4,
    't2.txt': 1,
  });

  // c1 recreate
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client1).toHaveIndexFileVersions({
    't1.txt': 5,
    't2.txt': 1,
  });
  expect(client2).toHaveIndexFileVersions({
    't1.txt': 5,
    't2.txt': 1,
  });
});

test('should sync mixture with two clients: update, delete and recreate files', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  // update t1.txt from c1
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();

  // delete t1.txt from c2
  delete files['t1.txt'];
  client2.deleteFiles(['t1.txt']);
  client2.run();
  client1.run();

  // recreate t1.txt from c1
  files['t1.txt'] = 'Recreate test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'Recreate test1!!!!!!' });
  client1.run();
  client2.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  const expectedFileVersions = {
    't1.txt': 4,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});

test('should sync mixture with three clients', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  client2.run();

  // update t1.txt from c1
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': files['t1.txt'] });

  client1.run();
  client2.run();

  // delete t1.txt from c2
  delete files['t1.txt'];
  client2.deleteFiles(['t1.txt']);
  client2.run();
  client1.run();

  const files3 = {
    't1.txt': 'This is test1 in c3',
    't2.txt': 'This is test2 in c3',
  };

  const client3 = getClient(files3);
  client3.run();

  expect(client1).toHaveExactLocalFiles(files);
  expect(client2).toHaveExactLocalFiles(files);
  expect(client3).toHaveExactLocalFiles(files);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client3).toHaveIndexFileHashesMatchLocalFileHashes();
  const expectedFileVersions1 = {
    't1.txt': 3,
    't2.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions1);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions1);
  expect(client3).toHaveIndexFileVersions(expectedFileVersions1);

  // update t1.txt from c3
  files3['t1.txt'] = 'This is new test1 in c3!!!!!!';
  client3.writeFiles({ 't1.txt': files3['t1.txt'] });
  delete files3['t2.txt'];
  client3.deleteFiles(['t2.txt']);

  client3.run();
  client2.run();

  expect(client2).toHaveExactLocalFiles(files3);
  expect(client3).toHaveExactLocalFiles(files3);
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client3).toHaveIndexFileHashesMatchLocalFileHashes();
  const expectedFileVersions2 = {
    't1.txt': 4,
    't2.txt': 2,
  };
  expect(client2).toHaveIndexFileVersions(expectedFileVersions2);
  expect(client3).toHaveIndexFileVersions(expectedFileVersions2);
});

test('should sync same file with different size (concurrent).', async () => {
  const files1 = {
    'testing.txt': Buffer.alloc(1024 * 1024 * 1024, '1'),
  };

  const files2 = {
    'testing.txt': Buffer.alloc(10, '2'),
  };

  const client1 = getClient(files1);
  const client2 = getClient(files2);

  // Client2 should win the update
  // And client1 should fetch the remote (client2's) version when failing to update its version
  await Promise.all([client1.runAsync(), client2.runAsync(10)]);

  expect(client1).toHaveExactLocalFiles(files2);
  expect(client2).toHaveExactLocalFiles(files2);
  expect(client1).toHaveIndexFileHashesMatchLocalFileHashes();
  expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
  const expectedFileVersions = {
    'testing.txt': 1,
  };
  expect(client1).toHaveIndexFileVersions(expectedFileVersions);
  expect(client2).toHaveIndexFileVersions(expectedFileVersions);
});
