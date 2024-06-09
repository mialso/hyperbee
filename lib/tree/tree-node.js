const b4a = require('b4a')
const safetyCatch = require('safety-catch')

const { sameValue } = require('../util.js')

const T = 5
const MIN_KEYS = T - 1
const MAX_CHILDREN = MIN_KEYS * 2 + 1

class Child {
  constructor (seq, offset, value) {
    this.seq = seq
    this.offset = offset
    this.value = value
  }
}

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

class TreeNode {
  constructor (block, keys, children, offset) {
    this.block = block
    this.offset = offset
    this.keys = keys
    this.children = children
    this.changed = false

    this.preload()
  }

  preload () {
    if (this.block === null) return

    const core = getBackingCore(this.block.tree.core)
    const indexedLength = getIndexedLength(this.block.tree.core)
    const bitfield = core.core.bitfield

    for (let i = 0; i < this.keys.length; i++) {
      const k = this.keys[i]
      if (k.value) continue
      if (k.seq >= indexedLength || bitfield.get(k.seq)) continue
      preloadBlock(core, k.seq)
    }
    for (let i = 0; i < this.children.length; i++) {
      const c = this.children[i]
      if (c.value) continue
      if (c.seq >= indexedLength || bitfield.get(c.seq)) continue
      preloadBlock(core, c.seq)
    }
  }

  async insertKey (key, value, child, node, casF) {
    let s = 0
    let e = this.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = b4a.compare(key.value, await this.getKey(mid))

      if (c === 0) {
        if (casF) {
          const prev = await this.getKeyNode(mid)
          if (!(await casF(prev, node))) return true
        }
        if (!this.block.tree.tree.alwaysDuplicate) {
          const prev = await this.getKeyNode(mid)
          if (sameValue(prev.value, value)) return true
        }
        this.changed = true
        this.keys[mid] = key
        return true
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    this.keys.splice(i, 0, key)
    if (child) this.children.splice(i + 1, 0, new Child(0, 0, child))
    this.changed = true

    return this.keys.length < MAX_CHILDREN
  }

  removeKey (index) {
    this.keys.splice(index, 1)
    if (this.children.length) {
      this.children[index + 1].seq = 0 // mark as freed
      this.children.splice(index + 1, 1)
    }
    this.changed = true
  }

  async siblings (parent) {
    for (let i = 0; i < parent.children.length; i++) {
      if (parent.children[i].value === this) {
        const left = i ? parent.getChildNode(i - 1) : null
        const right = i < parent.children.length - 1 ? parent.getChildNode(i + 1) : null
        return { left: await left, index: i, right: await right }
      }
    }

    throw new Error('Bad parent')
  }

  merge (node, median) {
    this.changed = true
    this.keys.push(median)
    for (let i = 0; i < node.keys.length; i++) this.keys.push(node.keys[i])
    for (let i = 0; i < node.children.length; i++) this.children.push(node.children[i])
  }

  async split () {
    const len = this.keys.length >> 1
    const right = TreeNode.create(this.block)

    while (right.keys.length < len) right.keys.push(this.keys.pop())
    right.keys.reverse()

    await this.getKey(this.keys.length - 1) // make sure the median is loaded
    const median = this.keys.pop()

    if (this.children.length) {
      while (right.children.length < len + 1) right.children.push(this.children.pop())
      right.children.reverse()
    }

    this.changed = true

    return {
      left: this,
      median,
      right
    }
  }

  getKeyNode (index) {
    return this.block.tree.getBlock(this.keys[index].seq)
  }

  async getChildNode (index) {
    const child = this.children[index]
    if (child.value) return child.value
    const block = child.seq === this.block.seq ? this.block : await this.block.tree.getBlock(child.seq)
    return (child.value = block.getTreeNode(child.offset))
  }

  setKey (index, key) {
    this.keys[index] = key
    this.changed = true
  }

  async getKey (index) {
    const key = this.keys[index]
    if (key.value) return key.value
    const k = key.seq === this.block.seq ? this.block.key : await this.block.tree.getKey(key.seq)
    return (key.value = k)
  }

  indexChanges (index, seq) {
    const offset = index.push(null) - 1
    this.changed = false

    for (const child of this.children) {
      if (!child.value || !child.value.changed) continue
      child.seq = seq
      child.offset = child.value.indexChanges(index, seq)
      index[child.offset] = child
    }

    return offset
  }

  updateChildren (seq, block) {
    for (const child of this.children) {
      if (!child.value || child.seq !== seq) continue
      child.value.block = block
      child.value.updateChildren(seq, block)
    }
  }

  static create (block) {
    const node = new TreeNode(block, [], [], 0)
    node.changed = true
    return node
  }
}

module.exports = { TreeNode, Child, MIN_KEYS }
