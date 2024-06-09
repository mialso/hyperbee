const b4a = require('b4a')
const { TreeNode, Child } = require('./tree-node.js')
const { sameValue } = require('../util.js')

async function addNode ({ rootNode, newNode, value, target, casF, alwaysDuplicate }) {
  let node = rootNode
  const stack = []

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
        if (casF) {
          const prev = await node.getKeyNode(mid)
          if (!(await casF(prev, newNode))) return null
        }
        if (!alwaysDuplicate) {
          const prev = await node.getKeyNode(mid)
          if (sameValue(prev.value, value)) return null
        }
        node.setKey(mid, target)
        return rootNode
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    const i = c < 0 ? e : s
    node = await node.getChildNode(i)
  }

  let needsSplit = !(await node.insertKey(target, value, null, newNode, casF))
  if (!node.changed) return null

  let newRoot = null
  while (needsSplit) {
    const parent = stack.pop()
    const { median, right } = await node.split()

    if (parent) {
      needsSplit = !(await parent.insertKey(median, value, right, null))
      node = parent
    } else {
      newRoot = TreeNode.create(node.block)
      newRoot.changed = true
      newRoot.keys.push(median)
      newRoot.children.push(new Child(0, 0, node), new Child(0, 0, right))
      needsSplit = false
    }
  }

  return newRoot || rootNode
}

module.exports = { addNode }
