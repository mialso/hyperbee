const Xache = require('xache')

class Cache {
  constructor () {
    this.keys = new Xache({ maxSize: 65536 })
    this.length = 0
  }

  get (seq) {
    return this.keys.get(seq) || null
  }

  set (seq, key) {
    this.keys.set(seq, key)
    if (seq >= this.length) this.length = seq + 1
  }

  gc (length) {
    // if we need to "work" more than 128 ticks, just bust the cache...
    if (this.length - length > 128) {
      this.keys.clear()
    } else {
      for (let i = length; i < this.length; i++) {
        this.keys.delete(i)
      }
    }

    this.length = length
  }
}

module.exports = { Cache }
