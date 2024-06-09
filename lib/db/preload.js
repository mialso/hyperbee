const safetyCatch = require('safety-catch')

function preloadBlock (core, index) {
  if (core.replicator._blocks.get(index)) return
  core.get(index).catch(safetyCatch)
}

function getBackingCore (core) {
  if (core._source) return core._source.originalCore
  if (core.flush) return core.session
  return core
}

function getIndexedLength (core) {
  if (core._source) return core._source.core.indexedLength
  if (core.flush) return core.indexedLength
  return core.length
}

function preloadNode ({ tree, keys, children }) {
  const core = getBackingCore(tree.core)
  const indexedLength = getIndexedLength(tree.core)
  const bitfield = core.core.bitfield

  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]
    if (k.value) continue
    if (k.seq >= indexedLength || bitfield.get(k.seq)) continue
    preloadBlock(core, k.seq)
  }
  for (let i = 0; i < children.length; i++) {
    const c = children[i]
    if (c.value) continue
    if (c.seq >= indexedLength || bitfield.get(c.seq)) continue
    preloadBlock(core, c.seq)
  }
}

module.exports = { preloadNode }
