const { runServer } = require('./libs/server');
const { waitForServerStart, waitForClientRun } = require('./libs/utils');

jest.setTimeout(100000);

let server; let getClient;

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
  await waitForClientRun();
  client2.run();
  await waitForClientRun();

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

test('should sync updates.', async () => {
  const files = {
    't1.txt': 'This is test1',
    't2.txt': 'This is test2',
  };

  const client1 = getClient(files);
  const client2 = getClient();

  client1.run();
  await waitForClientRun();
  client2.run();
  await waitForClientRun();

  files['t1.txt'] = 'This is new test1!!!!!!';
  client1.writeFiles({ 't1.txt': 'This is new test1!!!!!!' });

  client1.run();
  await waitForClientRun();
  client2.run();
  await waitForClientRun();

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
  await waitForClientRun();
  client2.run();
  await waitForClientRun();

  delete files['t1.txt'];
  client1.deleteFiles(['t1.txt']);

  client1.run();
  await waitForClientRun();
  client2.run();
  await waitForClientRun();

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
