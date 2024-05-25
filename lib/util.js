const b4a = require('b4a')

function sameValue (a, b) {
  return a === b || (a !== null && b !== null && b4a.equals(a, b))
}

module.exports = { sameValue }
