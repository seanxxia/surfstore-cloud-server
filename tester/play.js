const { runServer } = require('./testing/libs/server');
const { waitForServerStart } = require('./testing/libs/utils');

async function main() {
  server = runServer(4096);
  getClient = server.getClient;
  await waitForServerStart();

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

  console.log(c1Files, c2Files);

  await server.cleanup();
}


main();
