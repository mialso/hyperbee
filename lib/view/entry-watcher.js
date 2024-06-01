const ReadyResource = require('ready-resource')
const debounce = require('debounceify')
const safetyCatch = require('safety-catch')

class EntryWatcher extends ReadyResource {
  constructor (bee, key, opts = {}) {
    super()

    this.keyEncoding = opts.keyEncoding || bee.keyEncoding
    this.valueEncoding = opts.valueEncoding || bee.valueEncoding

    this.index = bee._entryWatchers.push(this) - 1
    this.bee = bee

    this.key = key
    this.node = null

    this._forceUpdate = false
    this._debouncedUpdate = debounce(this._processUpdate.bind(this))
    this._updateOnce = !!opts.updateOnce
  }

  _close () {
    const top = this.bee._entryWatchers.pop()
    if (top !== this) {
      top.index = this.index
      this.bee._entryWatchers[top.index] = top
    }
  }

  _onappend () {
    this._debouncedUpdate()
  }

  _ontruncate () {
    this._forceUpdate = true
    this._debouncedUpdate()
  }

  async _processUpdate () {
    const force = this._forceUpdate
    this._forceUpdate = false

    if (this._updateOnce) {
      this._updateOnce = false
      await this.bee.update({ wait: true })
    }

    let newNode
    try {
      newNode = await this.bee.get(this.key, {
        keyEncoding: this.keyEncoding,
        valueEncoding: this.valueEncoding
      })
    } catch (e) {
      if (e.code === 'SNAPSHOT_NOT_AVAILABLE') {
        // There was a truncate event before the get resolved
        // So this handler will run again anyway
        return
      } else if (this.bee.closing) {
        this.close().catch(safetyCatch)
        return
      }
      this.emit('error', e)
      return
    }

    if (force || newNode?.seq !== this.node?.seq) {
      this.node = newNode
      this.emit('update')
    }
  }
}

module.exports = { EntryWatcher }
