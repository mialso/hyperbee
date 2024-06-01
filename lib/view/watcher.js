const ReadyResource = require('ready-resource')
const mutexify = require('mutexify/promise')
const safetyCatch = require('safety-catch')

function autoFlowOnUpdate (name) {
  if (name === 'update') this._consume()
}

function defaultDiffer (currentSnap, previousSnap, opts) {
  return currentSnap.createDiffStream(previousSnap, opts)
}

function defaultWatchMap (snapshot) {
  return snapshot
}

class Watcher extends ReadyResource {
  constructor (bee, range, opts = {}) {
    super()

    this.keyEncoding = opts.keyEncoding || bee.keyEncoding
    this.valueEncoding = opts.valueEncoding || bee.valueEncoding
    this.index = bee._watchers.push(this) - 1
    this.bee = bee
    this.core = bee.core

    this.latestDiff = 0
    this.range = range
    this.map = opts.map || defaultWatchMap

    this.current = null
    this.previous = null
    this.currentMapped = null
    this.previousMapped = null
    this.stream = null

    this._lock = mutexify()
    this._flowing = false
    this._resolveOnChange = null
    this._differ = opts.differ || defaultDiffer
    this._eager = !!opts.eager
    this._updateOnce = !!opts.updateOnce
    this._onchange = opts.onchange || null
    this._flush = opts.flush !== false && this.core.isAutobase

    this.on('newListener', autoFlowOnUpdate)

    this.ready().catch(safetyCatch)
  }

  async _consume () {
    if (this._flowing) return
    try {
      for await (const _ of this) {} // eslint-disable-line
    } catch {}
  }

  async _open () {
    await this.bee.ready()

    const opts = {
      keyEncoding: this.keyEncoding,
      valueEncoding: this.valueEncoding
    }

    // Point from which to start watching
    this.current = this._eager ? this.bee.checkout(1, opts) : this.bee.snapshot(opts)

    if (this._onchange) {
      if (this._eager) await this._onchange()
      this._consume()
    }
  }

  [Symbol.asyncIterator] () {
    this._flowing = true
    return this
  }

  _ontruncate () {
    if (this.core.isAutobase) this._onappend()
  }

  _onappend () {
    // TODO: this is a light hack / fix for non-sparse session reporting .length's inside batches
    // the better solution is propably just to change non-sparse sessions to not report a fake length
    if (!this.core.isAutobase && (!this.core.core || this.core.core.tree.length !== this.core.length)) return

    const resolve = this._resolveOnChange
    this._resolveOnChange = null
    if (resolve) resolve()
  }

  async _waitForChanges () {
    if (this.current.version < this.bee.version || this.closing) return

    await new Promise(resolve => {
      this._resolveOnChange = resolve
    })
  }

  async next () {
    try {
      return await this._next()
    } catch (err) {
      if (this.closing) return { value: undefined, done: true }
      await this.close()
      throw err
    }
  }

  async _next () {
    const release = await this._lock()

    try {
      if (this.closing) return { value: undefined, done: true }

      if (!this.opened) await this.ready()

      while (true) {
        await this._waitForChanges()

        if (this._updateOnce) {
          this._updateOnce = false
          await this.bee.update({ wait: true })
        }

        if (this._flush) await this.core.base.flush()
        if (this.closing) return { value: undefined, done: true }

        await this._closePrevious()
        this.previous = this.current.snapshot()

        await this._closeCurrent()
        this.current = this.bee.snapshot({
          keyEncoding: this.keyEncoding,
          valueEncoding: this.valueEncoding
        })

        if (this.current.core.fork !== this.previous.core.fork) {
          return await this._yield()
        }

        this.stream = this._differ(this.current, this.previous, this.range)

        try {
          for await (const data of this.stream) { // eslint-disable-line
            return await this._yield()
          }
        } finally {
          this.stream = null
        }
      }
    } finally {
      release()
    }
  }

  async _yield () {
    this.currentMapped = this.map(this.current)
    this.previousMapped = this.map(this.previous)

    if (this._onchange) {
      try {
        await this._onchange()
      } catch (err) {
        safetyCatch(err)
      }
    }

    this.emit('update')
    return { done: false, value: [this.currentMapped, this.previousMapped] }
  }

  async return () {
    await this.close()
    return { done: true }
  }

  async _close () {
    const top = this.bee._watchers.pop()
    if (top !== this) {
      top.index = this.index
      this.bee._watchers[top.index] = top
    }

    if (this.stream && !this.stream.destroying) {
      this.stream.destroy()
    }

    this._onappend() // Continue execution being closed

    await this._closeCurrent().catch(safetyCatch)
    await this._closePrevious().catch(safetyCatch)

    const release = await this._lock()
    release()
  }

  destroy () {
    return this.close()
  }

  async _closeCurrent () {
    if (this.currentMapped) await this.currentMapped.close()
    if (this.current) await this.current.close()
    this.current = this.currentMapped = null
  }

  async _closePrevious () {
    if (this.previousMapped) await this.previousMapped.close()
    if (this.previous) await this.previous.close()
    this.previous = this.previousMapped = null
  }
}

module.exports = { Watcher }
