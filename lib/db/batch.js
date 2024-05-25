const b4a = require('b4a')
const codecs = require('codecs')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')

const { YoloIndex, Node, Header } = require('../messages')
const RangeIterator = require('../../iterators/range')

const { TreeNode, Child } = require('../tree/tree-node.js')
const { rebalance, setKeyToNearestLeaf } = require('../tree/operation.js')
const {
  sameValue, noop, enc, iteratorPeek, encRange, iteratorToStream
} = require('../util.js')
const { BlockEntry, Key } = require('./block.js')

function deflate (index) {
  const levels = index.map(l => {
    const keys = []
    const children = []

    for (let i = 0; i < l.value.keys.length; i++) {
      keys.push(l.value.keys[i].seq)
    }

    for (let i = 0; i < l.value.children.length; i++) {
      children.push(l.value.children[i].seq, l.value.children[i].offset)
    }

    return { keys, children }
  })

  return YoloIndex.encode({ levels })
}

class BatchEntry extends BlockEntry {
  constructor (seq, tree, key, value, index) {
    super(seq, tree, { key, value, index: null, inflated: null })
    this.pendingIndex = index
  }

  isTarget (key) {
    return false
  }

  getTreeNode (offset) {
    return this.pendingIndex[offset].value
  }
}

class Batch {
  constructor (tree, core, batchLock, cache, options = {}) {
    this.tree = tree
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = core
    this.core = core
    this.index = tree._batches.push(this) - 1
    this.blocks = cache ? new Map() : null
    this.autoFlush = !batchLock
    this.rootSeq = 0
    this.root = null
    this.length = 0
    this.options = options
    this.locked = null
    this.batchLock = batchLock
    this.onseq = this.options.onseq || noop
    this.appending = null
    this.isSnapshot = this.core !== this.tree.core
    this.shouldUpdate = this.options.update !== false
    this.updating = null
    this.encoding = {
      key: options.keyEncoding ? codecs(options.keyEncoding) : tree.keyEncoding,
      value: options.valueEncoding ? codecs(options.valueEncoding) : tree.valueEncoding
    }
  }

  ready () {
    return this.tree.ready()
  }

  async lock () {
    if (this.tree.readonly) throw new Error('Hyperbee is marked as read-only')
    if (this.locked === null) this.locked = await this.tree.lock()
  }

  get version () {
    return Math.max(1, this.tree._checkout ? this.tree._checkout : this.core.length + this.length)
  }

  async getRoot (ensureHeader) {
    await this.ready()
    if (ensureHeader) {
      if (this.core.length === 0 && this.core.writable && !this.tree.readonly) {
        await this.core.append(Header.encode({
          protocol: 'hyperbee',
          metadata: this.tree.metadata
        }))
      }
    }
    if (this.tree._checkout === 0 && this.shouldUpdate) {
      if (this.updating === null) this.updating = this.core.update()
      await this.updating
    }
    if (this.version < 2) return null
    return (await this.getBlock(this.version - 1)).getTreeNode(0)
  }

  async getKey (seq) {
    const k = this.core.fork === this.tree.core.fork ? this.tree._keyCache.get(seq) : null
    if (k !== null) return k
    const key = (await this.getBlock(seq)).key
    if (this.core.fork === this.tree.core.fork) this.tree._keyCache.set(seq, key)
    return key
  }

  async _getNode (seq) {
    const cached = this.core.fork === this.tree.core.fork ? this.tree._nodeCache.get(seq) : null
    if (cached !== null) return cached
    const entry = await this.core.get(seq, { ...this.options, valueEncoding: Node })
    if (entry === null) throw BLOCK_NOT_AVAILABLE()
    const wrap = {
      key: entry.key,
      value: entry.value,
      index: entry.index,
      inflated: null
    }
    if (this.core.fork === this.tree.core.fork) this.tree._nodeCache.set(seq, wrap)
    return wrap
  }

  async getBlock (seq) {
    if (this.rootSeq === 0) this.rootSeq = seq
    let b = this.blocks && this.blocks.get(seq)
    if (b) return b
    this.onseq(seq)
    const entry = await this._getNode(seq)
    b = new BlockEntry(seq, this, entry)
    if (this.blocks && (this.blocks.size - this.length) < 128) this.blocks.set(seq, b)
    return b
  }

  _onwait (key) {
    this.options.onwait = null
    this.tree.extension.get(this.rootSeq + 1, key)
  }

  _getEncoding (opts) {
    if (!opts) return this.encoding
    return {
      key: opts.keyEncoding ? codecs(opts.keyEncoding) : this.encoding.key,
      value: opts.valueEncoding ? codecs(opts.valueEncoding) : this.encoding.value
    }
  }

  peek (range, opts) {
    return iteratorPeek(this.createRangeIterator(range, { ...opts, limit: 1 }))
  }

  createRangeIterator (range, opts = {}) {
    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const encoding = this._getEncoding(opts)
    return new RangeIterator(this, encoding, encRange(encoding.key, { ...opts, sub: this.tree._sub }))
  }

  createReadStream (range, opts) {
    const signal = (opts && opts.signal) || null
    return iteratorToStream(this.createRangeIterator(range, opts), signal)
  }

  async getBySeq (seq, opts) {
    const encoding = this._getEncoding(opts)

    try {
      const block = (await this.getBlock(seq)).final(encoding)
      return { key: block.key, value: block.value }
    } finally {
      await this._closeSnapshot()
    }
  }

  async get (key, opts) {
    const encoding = this._getEncoding(opts)

    try {
      return await this._get(key, encoding)
    } finally {
      await this._closeSnapshot()
    }
  }

  async _get (key, encoding) {
    key = enc(encoding.key, key)

    if (this.tree.extension !== null && this.options.extension !== false) {
      this.options.onwait = this._onwait.bind(this, key)
    }

    let node = await this.getRoot(false)
    if (!node) return null

    while (true) {
      if (node.block.isTarget(key)) {
        return node.block.isDeletion() ? null : node.block.final(encoding)
      }

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1

        c = b4a.compare(key, await node.getKey(mid))

        if (c === 0) return (await this.getBlock(node.keys[mid].seq)).final(encoding)

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.children.length) return null

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }
  }

  async put (key, value, opts) {
    const release = this.batchLock ? await this.batchLock() : null

    const cas = (opts && opts.cas) || null
    const encoding = this._getEncoding(opts)

    if (!this.locked) await this.lock()
    if (!release) return this._put(key, value, encoding, cas)

    try {
      return await this._put(key, value, encoding, cas)
    } finally {
      release()
    }
  }

  async _put (key, value, encoding, cas) {
    const newNode = {
      seq: 0,
      key,
      value
    }
    key = enc(encoding.key, key)
    value = enc(encoding.value, value)

    const stack = []

    let root
    let node = root = await this.getRoot(true)
    if (!node) node = root = TreeNode.create(null)

    const seq = newNode.seq = this.core.length + this.length
    const target = new Key(seq, key)

    while (node.children.length) {
      stack.push(node)
      node.changed = true // changed, but compressible

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(target.value, await node.getKey(mid))

        if (c === 0) {
          if (cas) {
            const prev = await node.getKeyNode(mid)
            if (!(await cas(prev.final(encoding), newNode))) return this._unlockMaybe()
          }
          if (!this.tree.alwaysDuplicate) {
            const prev = await node.getKeyNode(mid)
            if (sameValue(prev.value, value)) return this._unlockMaybe()
          }
          node.setKey(mid, target)
          return this._append(root, seq, key, value)
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }

    let needsSplit = !(await node.insertKey(target, value, null, newNode, encoding, cas))
    if (!node.changed) return this._unlockMaybe()

    while (needsSplit) {
      const parent = stack.pop()
      const { median, right } = await node.split()

      if (parent) {
        needsSplit = !(await parent.insertKey(median, value, right, null, encoding, null))
        node = parent
      } else {
        root = TreeNode.create(node.block)
        root.changed = true
        root.keys.push(median)
        root.children.push(new Child(0, 0, node), new Child(0, 0, right))
        needsSplit = false
      }
    }

    return this._append(root, seq, key, value)
  }

  async del (key, opts) {
    const release = this.batchLock ? await this.batchLock() : null
    const cas = (opts && opts.cas) || null
    const encoding = this._getEncoding(opts)

    if (!this.locked) await this.lock()
    if (!release) return this._del(key, encoding, cas)

    try {
      return await this._del(key, encoding, cas)
    } finally {
      release()
    }
  }

  async _del (key, encoding, cas) {
    const delNode = {
      seq: 0,
      key,
      value: null
    }

    key = enc(encoding.key, key)

    const stack = []

    let node = await this.getRoot(true)
    if (!node) return this._unlockMaybe()

    const seq = delNode.seq = this.core.length + this.length

    while (true) {
      stack.push(node)

      let s = 0
      let e = node.keys.length
      let c

      while (s < e) {
        const mid = (s + e) >> 1
        c = b4a.compare(key, await node.getKey(mid))

        if (c === 0) {
          if (cas) {
            const prev = await node.getKeyNode(mid)
            if (!(await cas(prev.final(encoding), delNode))) return this._unlockMaybe()
          }
          if (node.children.length) await setKeyToNearestLeaf(node, mid, stack)
          else node.removeKey(mid)
          // we mark these as changed late, so we don't rewrite them if it is a 404
          for (const node of stack) node.changed = true
          return this._append(await rebalance(stack), seq, key, null)
        }

        if (c < 0) e = mid
        else s = mid + 1
      }

      if (!node.children.length) return this._unlockMaybe()

      const i = c < 0 ? e : s
      node = await node.getChildNode(i)
    }
  }

  async _closeSnapshot () {
    if (this.isSnapshot) {
      await this.core.close()
      this._finalize()
    }
  }

  async close () {
    if (this.isSnapshot) return this._closeSnapshot()

    this.root = null
    if (this.blocks) this.blocks.clear()
    this.length = 0
    this._unlock()
  }

  destroy () { // compat, remove later
    this.close().catch(noop)
  }

  toBlocks () {
    if (this.appending) return this.appending

    const batch = new Array(this.length)

    for (let i = 0; i < this.length; i++) {
      const seq = this.core.length + i
      const { pendingIndex, key, value } = this.blocks.get(seq)

      if (i < this.length - 1) {
        pendingIndex[0] = null
        let j = 0

        while (j < pendingIndex.length) {
          const idx = pendingIndex[j]
          if (idx !== null && idx.seq === seq) {
            idx.offset = j++
            continue
          }
          if (j === pendingIndex.length - 1) pendingIndex.pop()
          else pendingIndex[j] = pendingIndex.pop()
        }
      }

      batch[i] = Node.encode({
        key,
        value,
        index: deflate(pendingIndex)
      })
    }

    this.appending = batch
    return batch
  }

  flush () {
    if (!this.length) return this.close()

    const batch = this.toBlocks()

    this.root = null
    this.blocks.clear()
    this.length = 0

    return this._appendBatch(batch)
  }

  _unlockMaybe () {
    if (this.autoFlush) this._unlock()
  }

  _unlock () {
    const locked = this.locked
    this.locked = null
    if (locked !== null) locked()
    this._finalize()
  }

  _finalize () {
    // technically finalize can be called more than once, so here we just check if we already have been removed
    if (this.index >= this.tree._batches.length || this.tree._batches[this.index] !== this) return
    const top = this.tree._batches.pop()
    if (top === this) return
    top.index = this.index
    this.tree._batches[top.index] = top
  }

  _append (root, seq, key, value) {
    const index = []
    root.indexChanges(index, seq)
    index[0] = new Child(seq, 0, root)

    if (!this.autoFlush) {
      const block = new BatchEntry(seq, this, key, value, index)
      root.block = block
      this.root = root
      this.length++
      this.blocks.set(seq, block)

      root.updateChildren(seq, block)
      return
    }

    return this._appendBatch(Node.encode({
      key,
      value,
      index: deflate(index)
    }))
  }

  async _appendBatch (raw) {
    try {
      await this.core.append(raw)
    } finally {
      this._unlock()
    }
  }
}

module.exports = { Batch }
