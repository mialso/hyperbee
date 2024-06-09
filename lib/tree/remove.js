const b4a = require('b4a')
const { setKeyToNearestLeaf } = require('./operation.js')

async function removeNode ({ rootNode, delNode, key, casF }) {
  const stack = []
  let node = rootNode

  while (true) {
    stack.push(node)

    let s = 0
    let e = node.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1
      c = b4a.compare(key, await node.getKey(mid))

      if (c === 0) {
        if (casF) {
          const prev = await node.getKeyNode(mid)
          if (!(await casF(prev, delNode))) return null
        }
        if (node.children.length) await setKeyToNearestLeaf(node, mid, stack)
        else node.removeKey(mid)
        // we mark these as changed late, so we don't rewrite them if it is a 404
        for (const node of stack) node.changed = true
        return stack
      }

      if (c < 0) e = mid
      else s = mid + 1
    }

    if (!node.children.length) return null

    const i = c < 0 ? e : s
    node = await node.getChildNode(i)
  }
}

module.exports = { removeNode }
