const { runServer } = require('./libs/server');
const { waitForServerStart } = require('./libs/utils');

// const blockSizes = [4096];
const blockSizes = [
  4096,
  8192,
  1024 * 1024, // spec: block size of 1 mega byte
];

for (const blockSize of blockSizes) {
  describe(`Block size = ${blockSize}`, () => {
    let server;
    let getClient;

    beforeEach(async () => {
      server = runServer(blockSize);
      getClient = server.getClient;
      await waitForServerStart();
    });

    afterEach(async () => {
      await server.cleanup();
    });

    test('should sync files (large binary).', async () => {
      const files = {
        'video.mp4': ({ copy }) => {
          // Copy from file in dir "testing/fixture"
          copy('video.mp4');
        },
        'large.txt': ({ write }) => {
          // Write string (buffer) to file
          write(Buffer.alloc(1024 * 512, 'a'));
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

    test('should sync same file with different size (concurrent).', async () => {
      const files1 = {
        'testing.txt': Buffer.alloc(1024 * 512, '1'),
      };

      const files2 = {
        'testing.txt': Buffer.alloc(10, '2'),
      };

      const client1 = getClient(files1);
      const client2 = getClient(files2);

      // Client2 should win the update
      // And client1 should fetch the remote update (client2's update) when failing to upload its update
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

    test('should sync file with a correct version number and file while concurrently upload file', async () => {
      const serverfile = {
        'testing.txt': 'I am in the server',
      };
      const client1 = getClient(serverfile);
      client1.run();

      const updatefile = {
        'testing.txt': Buffer.alloc(1024 * 512, '1'),
      };
      client1.writeFiles({ 'testing.txt': updatefile['testing.txt'] });

      const client2 = getClient();
      // Client2 should download the old server texting.txt with the old version
      await Promise.all([client1.runAsync(), client2.runAsync(10)]);

      expect(client2).toHaveExactLocalFiles(serverfile);
      expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
      let expectedFileVersions = {
        'testing.txt': 1,
      };
      expect(client2).toHaveIndexFileVersions(expectedFileVersions);

      // Client 2 should download the newest texting.txt with new version
      client2.run();
      expect(client2).toHaveExactLocalFiles(updatefile);
      expect(client2).toHaveIndexFileHashesMatchLocalFileHashes();
      expectedFileVersions = {
        'testing.txt': 2,
      };
      expect(client2).toHaveIndexFileVersions(expectedFileVersions);
    });
  });
}
