var { EventEmitter } = require('events')
var { Readable } = require('stream')

module.exports = function viewQuery (sq) {
  var counter = {}, heads = {}
  var events = new EventEmitter
  return {
    map: function (msgs, next) {
      msgs = msgs.filter(msg => msg.value && msg.value.type === 'counter')
      msgs.forEach((msg) => {
        if (msg.value.n > (counter[msg.value.key] || 0)) {
          heads[msg.value.key] = { key: msg.key, seq: msg.seq }
          counter[msg.value.key] = msg.value.n
          events.emit('counter', msg.value.key, counter[msg.value.key])
        }
      })
      next()
    },
    api: {
      events,
      get (key) { return counter[key] },
      add (kcore, key, n, cb) {
        sq.feeds.getOrCreateLocal('default', { valueEncoding: 'json' }, onfeed)
        function onfeed (err, feed) {
          if (err) return cb(err)
          else feed.append({ type: 'counter', key, n: (counter[key] || 0) + n }, cb)
        }
      }
    },
    query: {
      api: {
        counter: function () {
          var r = new Readable({ objectMode: true, read: function () {} })
          r.close = function () {
            events.removeListener('counter', oncount)
          }
          Object.keys(heads).forEach(fkey => r.push(heads[fkey]))
          events.on('counter', oncount)
          return r
          function oncount (key, n) { r.push(heads[key]) }
        }
      },
      replicate: function ({ query }) {
        query('counter')
      }
    }
  }
}
