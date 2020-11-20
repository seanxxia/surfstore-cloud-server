const fs = require('fs');
const path = require('path');
const { runServer } = require('./libs/server');
const { waitForServerStart, areBuffersEqual } = require('./libs/utils');

jest.setTimeout(100000);

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

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    // Use `.content` to access the content of file in string
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
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

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
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

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  // Use `.contents` to access the content of file in buffer (bytes)
  expect(areBuffersEqual(c1Files['large.txt'].contents, c2Files['large.txt'].contents)).toBeTruthy();
  expect(areBuffersEqual(c1Files['video.mp4'].contents, c2Files['video.mp4'].contents)).toBeTruthy();

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
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

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
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

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
});

test('should sync empty files', async () => { });

  client1.run();
  client2.run();

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
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

  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
});

test('should sync update to empty files', async () => {
  
});

test('should sync append/delete content from file', async () => {});

test.only('should sync create delete recreate delete recreate.', async () => {
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

  let c1Files = client1.readFiles();
  let c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

  // c2 delete
  delete files['t1.txt'];
  client2.deleteFiles(['t1.txt']);

  client2.run();
  client1.run();

  c1Files = client1.readFiles();
  c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();

  // c1 recreate
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();

  c1Files = client1.readFiles();
  c2Files = client2.readFiles();
  Object.keys(c1Files) 
  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
});

test('should sync mixture with two clients: update, delete and recreate files', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
});
  const client2 = getClient();

  client1.run();
  client2.run();

  // update t1.txt from c1
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  client2.run();
  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();
  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
  // delete t1.txt from c2
  delete files['t2.txt'];
  client2.deleteFiles(['t2.txt']);
  client2.run();
  client1.run();
  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();
  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }
  // recreate t1.txt from c1
  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'Recreate test1!!!!!!' });
  client1.run();
  client2.run();
  c1Files = client1.readFiles();
  c2Files = client2.readFiles();
  expect(c1Files.length).toBe(c2Files.length);
  for (const [fname, content] of Object.entries(files)) {
    expect(c1Files[fname]).toBeDefined();
    expect(c2Files[fname]).toBeDefined();
    expect(c1Files[fname].content).toBe(content);
    expect(c2Files[fname].content).toBe(content);
  }

test('should sync same file with different size (concurrent).', async () => {
  const files1 = {
    'testing.txt': Buffer.alloc(1024 * 1024 * 1024, '1'),
  };

  const files2 = {
    'testing.txt': Buffer.alloc(10, '2'),
  };

  const client1 = getClient(files1);
  const client2 = getClient(files2);

  // Just a toy example
  await Promise.all([client1.runAsync(), client2.runAsync(10)]);

  const c1Files = client1.readFiles();
  const c2Files = client2.readFiles();

  expect(c1Files.length).toBe(c2Files.length);
  // expect(areBuffersEqual(c1Files['testing.txt'].contents, c2Files['testing.txt'].contents)).toBeTruthy();
  expect(areBuffersEqual(c1Files['testing.txt'].contents, files2['testing.txt'])).toBeTruthy();

  expect(client1.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
  expect(client2.isIndexFileHashesMatchLocalFileHashes()).toBeTruthy();
});
