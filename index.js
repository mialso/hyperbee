const codecs = require('codecs')
const mutexify = require('mutexify/promise')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')

const RangeIterator = require('./iterators/range')
const HistoryIterator = require('./iterators/history')
const DiffIterator = require('./iterators/diff')
const Extension = require('./lib/extension')
const { Header } = require('./lib/messages')
const { BLOCK_NOT_AVAILABLE } = require('hypercore-errors')

const { EntryWatcher } = require('./lib/view/entry-watcher.js')
const { Watcher } = require('./lib/view/watcher.js')
const { Batch } = require('./lib/db/batch.js')
const { Cache } = require('./lib/cache.js')
const {
  enc, iteratorPeek, encRange, SEP, EMPTY, iteratorToStream
} = require('./lib/util.js')

class Hyperbee extends ReadyResource {
  constructor (core, opts = {}) {
    super()
    // this.feed is now deprecated, and will be this.core going forward
    this.feed = core
    this.core = core

    this.keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : null
    this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
    this.extension = opts.extension !== false ? opts.extension || Extension.register(this) : null
    this.metadata = opts.metadata || null
    this.lock = opts.lock || mutexify()
    this.sep = opts.sep || SEP
    this.readonly = !!opts.readonly
    this.prefix = opts.prefix || null

    // In a future version, this should be false by default
    this.alwaysDuplicate = opts.alwaysDuplicate !== false

    this._unprefixedKeyEncoding = this.keyEncoding
    this._sub = !!this.prefix
    this._checkout = opts.checkout || 0
    this._view = !!opts._view

    this._onappendBound = this._view ? null : this._onappend.bind(this)
    this._ontruncateBound = this._view ? null : this._ontruncate.bind(this)
    this._watchers = this._onappendBound ? [] : null
    this._entryWatchers = this._onappendBound ? [] : null
    this._sessions = opts.sessions !== false
    this._keyCache = new Cache()
    this._nodeCache = new Cache()

    this._batches = []

    if (this._watchers) {
      this.core.on('append', this._onappendBound)
      this.core.on('truncate', this._ontruncateBound)
    }

    if (this.prefix && opts._sub) {
      this.keyEncoding = prefixEncoding(this.prefix, this.keyEncoding)
    }
  }

  _open () {
    return this.core.ready()
  }

  get version () {
    return Math.max(1, this._checkout || this.core.length)
  }

  get id () {
    return this.core.id
  }

  get key () {
    return this.core.key
  }

  get discoveryKey () {
    return this.core.discoveryKey
  }

  get writable () {
    return this.core.writable
  }

  get readable () {
    return this.core.readable
  }

  replicate (isInitiator, opts) {
    return this.core.replicate(isInitiator, opts)
  }

  update (opts) {
    return this.core.update(opts)
  }

  peek (range, opts) {
    return iteratorPeek(this.createRangeIterator(range, { ...opts, limit: 1 }))
  }

  createRangeIterator (range, opts = {}) {
    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const extension = (opts.extension === false && opts.limit !== 0) ? null : this.extension
    const keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : this.keyEncoding

    if (extension) {
      const { onseq, onwait } = opts
      let version = 0
      let next = 0

      opts = encRange(keyEncoding, {
        ...opts,
        sub: this._sub,
        onseq (seq) {
          if (!version) version = seq + 1
          if (next) next--
          if (onseq) onseq(seq)
        },
        onwait (seq) {
          if (!next) {
            next = Extension.BATCH_SIZE
            extension.iterator(ite.snapshot(version))
          }
          if (onwait) onwait(seq)
        }
      })
    } else {
      opts = encRange(keyEncoding, { ...opts, sub: this._sub })
    }

    const ite = new RangeIterator(new Batch(this, this._makeSnapshot(), null, false, opts), null, opts)
    return ite
  }

  createReadStream (range, opts) {
    const signal = (opts && opts.signal) || null
    return iteratorToStream(this.createRangeIterator(range, opts), signal)
  }

  createHistoryStream (opts) {
    const session = (opts && opts.live) ? this.core.session() : this._makeSnapshot()
    const signal = (opts && opts.signal) || null
    return iteratorToStream(new HistoryIterator(new Batch(this, session, null, false, opts), opts), signal)
  }

  createDiffStream (right, range, opts) {
    if (typeof right === 'number') right = this.checkout(Math.max(1, right), { reuseSession: true })

    // backwards compat range arg
    opts = opts ? { ...opts, ...range } : range

    const snapshot = right.version > this.version ? right._makeSnapshot() : this._makeSnapshot()
    const signal = (opts && opts.signal) || null

    const keyEncoding = opts && opts.keyEncoding ? codecs(opts.keyEncoding) : this.keyEncoding
    if (keyEncoding) opts = encRange(keyEncoding, { ...opts, sub: this._sub })

    return iteratorToStream(new DiffIterator(new Batch(this, snapshot, null, false, opts), new Batch(right, snapshot, null, false, opts), opts), signal)
  }

  get (key, opts) {
    const b = new Batch(this, this._makeSnapshot(), null, true, opts)
    return b.get(key)
  }

  getBySeq (seq, opts) {
    const b = new Batch(this, this._makeSnapshot(), null, true, opts)
    return b.getBySeq(seq)
  }

  put (key, value, opts) {
    const b = new Batch(this, this.core, null, true, opts)
    return b.put(key, value, opts)
  }

  batch (opts) {
    return new Batch(this, this.core, mutexify(), true, opts)
  }

  del (key, opts) {
    const b = new Batch(this, this.core, null, true, opts)
    return b.del(key, opts)
  }

  watch (range, opts) {
    if (!this._watchers) throw new Error('Can only watch the main bee instance')
    return new Watcher(this, range, opts)
  }

  async getAndWatch (key, opts) {
    if (!this._watchers) throw new Error('Can only watch the main bee instance')

    const watcher = new EntryWatcher(this, key, opts)
    await watcher._debouncedUpdate()

    if (this.closing) {
      await watcher.close()
      throw new Error('Bee closed')
    }

    return watcher
  }

  _onappend () {
    for (const watcher of this._watchers) {
      watcher._onappend()
    }

    for (const watcher of this._entryWatchers) {
      watcher._onappend()
    }
  }

  _ontruncate () {
    for (const watcher of this._watchers) {
      watcher._ontruncate()
    }

    for (const watcher of this._entryWatchers) {
      watcher._ontruncate()
    }

    this._nodeCache.gc(this.core.length)
    this._keyCache.gc(this.core.length)
  }

  _makeSnapshot () {
    if (this._sessions === false) return this.core
    // TODO: better if we could encapsulate this in hypercore in the future
    return (this._checkout <= this.core.length || this._checkout <= 1) ? this.core.snapshot() : this.core.session({ snapshot: false })
  }

  checkout (version, opts = {}) {
    if (version < 1) version = 1

    // same as above, just checkout isn't set yet...
    const snap = (opts.reuseSession || this._sessions === false)
      ? this.core
      : (version <= this.core.length || version <= 1) ? this.core.snapshot() : this.core.session({ snapshot: false })

    return new Hyperbee(snap, {
      _view: true,
      _sub: false,
      prefix: this.prefix,
      sep: this.sep,
      lock: this.lock,
      checkout: version,
      keyEncoding: opts.keyEncoding || this.keyEncoding,
      valueEncoding: opts.valueEncoding || this.valueEncoding,
      extension: this.extension !== null ? this.extension : false
    })
  }

  snapshot (opts) {
    return this.checkout(Math.max(1, this.version), opts)
  }

  sub (prefix, opts = {}) {
    let sep = opts.sep || this.sep
    if (!b4a.isBuffer(sep)) sep = b4a.from(sep)

    prefix = b4a.concat([this.prefix || EMPTY, b4a.from(prefix), sep])

    const valueEncoding = codecs(opts.valueEncoding || this.valueEncoding)
    const keyEncoding = codecs(opts.keyEncoding || this._unprefixedKeyEncoding)

    return new Hyperbee(this.core, {
      _view: true,
      _sub: true,
      prefix,
      sep: this.sep,
      lock: this.lock,
      checkout: this._checkout,
      valueEncoding,
      keyEncoding,
      extension: this.extension !== null ? this.extension : false,
      metadata: this.metadata
    })
  }

  async getHeader (opts) {
    const blk = await this.core.get(0, opts)
    return blk && Header.decode(blk)
  }

  async _close () {
    if (this._watchers) {
      this.core.off('append', this._onappendBound)
      this.core.off('truncate', this._ontruncateBound)

      while (this._watchers.length) {
        await this._watchers[this._watchers.length - 1].close()
      }
    }

    if (this._entryWatchers) {
      while (this._entryWatchers.length) {
        await this._entryWatchers[this._entryWatchers.length - 1].close()
      }
    }

    while (this._batches.length) {
      await this._batches[this._batches.length - 1].close()
    }

    return this.core.close()
  }

  static async isHyperbee (core, opts) {
    await core.ready()

    const blk0 = await core.get(0, opts)
    if (blk0 === null) throw BLOCK_NOT_AVAILABLE()

    try {
      return Header.decode(blk0).protocol === 'hyperbee'
    } catch (err) { // undecodable
      return false
    }
  }
}

function prefixEncoding (prefix, keyEncoding) {
  return {
    encode (key) {
      return b4a.concat([prefix, b4a.isBuffer(key) ? key : enc(keyEncoding, key)])
    },
    decode (key) {
      const sliced = key.slice(prefix.length, key.length)
      return keyEncoding ? keyEncoding.decode(sliced) : sliced
    }
  }
}

module.exports = Hyperbee
