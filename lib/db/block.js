const b4a = require('b4a')

const { TreeNode, Child } = require('../tree/tree-node.js')
const { YoloIndex } = require('../messages')
const { preloadNode } = require('./preload.js')

class Key {
  constructor (seq, value) {
    this.seq = seq
    this.value = value
  }
}

class Pointers {
  constructor (decoded) {
    this.levels = decoded.levels.map(l => {
      const children = []
      const keys = []

      for (let i = 0; i < l.keys.length; i++) {
        keys.push(new Key(l.keys[i], null))
      }

      for (let i = 0; i < l.children.length; i += 2) {
        children.push(new Child(l.children[i], l.children[i + 1], null))
      }

      return { keys, children }
    })
  }

  get (i) {
    return this.levels[i]
  }

  hasKey (seq) {
    for (const lvl of this.levels) {
      for (const key of lvl.keys) {
        if (key.seq === seq) return true
      }
    }
    return false
  }
}

function inflate (entry) {
  if (entry.inflated === null) {
    entry.inflated = YoloIndex.decode(entry.index)
    entry.index = null
  }
  return new Pointers(entry.inflated)
}

class BlockEntry {
  constructor (seq, tree, entry) {
    this.seq = seq
    this.tree = tree
    this.index = null
    this.entry = entry
    this.key = entry.key
    this.value = entry.value
  }

  isTarget (key) {
    return b4a.equals(this.key, key)
  }

  isDeletion () {
    if (this.value !== null) return false

    if (this.index === null) {
      this.index = inflate(this.entry)
    }

    return !this.index.hasKey(this.seq)
  }

  final (encoding) {
    return {
      seq: this.seq,
      key: encoding.key ? encoding.key.decode(this.key) : this.key,
      value: this.value && (encoding.value ? encoding.value.decode(this.value) : this.value)
    }
  }

  getTreeNode (offset) {
    if (this.index === null) {
      this.index = inflate(this.entry)
    }
    const entry = this.index.get(offset)
    const node = new TreeNode(this, entry.keys, entry.children, offset)
    preloadNode({ tree: this.tree, keys: entry.keys, children: entry.children })
    return node
  }
}

module.exports = { BlockEntry, Key }
