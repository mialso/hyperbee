const b4a = require('b4a')

async function getNode ({ rootNode, key, predicate }) {
  let node = rootNode

  while (true) {
    if (predicate(node)) {
      return { node, target: true }
    }

    let s = 0
    let e = node.keys.length
    let c

    while (s < e) {
      const mid = (s + e) >> 1

      c = b4a.compare(key, await node.getKey(mid))

      if (c === 0) return { node, mid }

      if (c < 0) e = mid
      else s = mid + 1
    }

    if (!node.children.length) return {}

    const i = c < 0 ? e : s
    node = await node.getChildNode(i)
  }
}

module.exports = { getNode }
