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
  await Promise.all([client1.runAsync(), client2.runAsync()]);

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
});
