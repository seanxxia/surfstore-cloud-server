function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(() => resolve(), milliseconds));
}
module.exports.sleep = sleep;

async function waitForClientRun() {
  await sleep(800);
}
module.exports.waitForClientRun = waitForClientRun;

async function waitForServerStart() {
  await sleep(800);
}
module.exports.waitForServerStart = waitForServerStart;

function areBuffersEqual(bufA, bufB) {
  if (!(bufA instanceof Buffer) || !(bufB instanceof Buffer)) {
    return false;
  }

  const len = bufA.length;
  if (len !== bufB.length) {
    return false;
  }
  for (let i = 0; i < len; i++) {
    if (bufA.readUInt8(i) !== bufB.readUInt8(i)) {
      return false;
    }
  }
  return true;
}
module.exports.areBuffersEqual = areBuffersEqual;
