const b4a = require('b4a')
const { Readable } = require('streamx')

function sameValue (a, b) {
  return a === b || (a !== null && b !== null && b4a.equals(a, b))
}

function noop () {}

function enc (e, v) {
  if (v === undefined || v === null) return null
  if (e !== null) return e.encode(v)
  if (typeof v === 'string') return b4a.from(v)
  return v
}

async function iteratorPeek (ite) {
  try {
    await ite.open()
    return await ite.next()
  } finally {
    await ite.close()
  }
}

const SEP = b4a.alloc(1)
const EMPTY = b4a.alloc(0)

function bump (key) {
  // key should have been copied by enc above before hitting this
  key[key.length - 1]++
  return key
}

function encRange (e, opts) {
  if (!e) return opts

  if (e.encodeRange) {
    const r = e.encodeRange({ gt: opts.gt, gte: opts.gte, lt: opts.lt, lte: opts.lte })
    opts.gt = r.gt
    opts.gte = r.gte
    opts.lt = r.lt
    opts.lte = r.lte
    return opts
  }

  if (opts.gt !== undefined) opts.gt = enc(e, opts.gt)
  if (opts.gte !== undefined) opts.gte = enc(e, opts.gte)
  if (opts.lt !== undefined) opts.lt = enc(e, opts.lt)
  if (opts.lte !== undefined) opts.lte = enc(e, opts.lte)
  if (opts.sub && !opts.gt && !opts.gte) opts.gt = enc(e, SEP)
  if (opts.sub && !opts.lt && !opts.lte) opts.lt = bump(enc(e, EMPTY))

  return opts
}

function iteratorToStream (ite, signal) {
  let done
  let closing

  const rs = new Readable({
    signal,
    open (cb) {
      done = cb
      ite.open().then(fin, fin)
    },
    read (cb) {
      done = cb
      ite.next().then(push, fin)
    },
    predestroy () {
      closing = ite.close()
      closing.catch(noop)
    },
    destroy (cb) {
      done = cb
      if (!closing) closing = ite.close()
      closing.then(fin, fin)
    }
  })

  return rs

  function fin (err) {
    done(err)
  }

  function push (val) {
    rs.push(val)
    done(null)
  }
}

module.exports = { sameValue, noop, enc, iteratorPeek, encRange, SEP, EMPTY, iteratorToStream }
