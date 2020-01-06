var memdb = require('memdb')
var ram = require('random-access-memory')
var { EventEmitter } = require('events')
var { Readable } = require('stream')

var SQ = require('../')
var sq = {
  A: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram }),
  B: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram })
}
var Kappa = require('kappa-core')
var core = { A: new Kappa, B: new Kappa }
var vq = { A: viewQuery(sq.A), B: viewQuery(sq.B) }
sq.A.use('counter', vq.A.query)
sq.B.use('counter', vq.B.query)
core.A.use('counter', sq.A.source(), vq.A)
core.B.use('counter', sq.B.source(), vq.B)

core.A.view.counter.events
  .on('counter', (key,n) => console.log(`A:${key}=${n}`))
core.B.view.counter.events
  .on('counter', (key,n) => console.log(`B:${key}=${n}`))

process.nextTick(function f () {
  var key = String.fromCharCode(97 + Math.floor(Math.random()*5))
  var n = Math.floor(Math.random()*10)
  if (Math.random() > 0.5) {
    core.A.view.counter.add(key, n, next)
  } else {
    core.B.view.counter.add(key, n, next)
  }
  function next (err) {
    if (err) console.error(err)
    else setTimeout(f, 1000)
  }
})

var rA = sq.A.replicate(true)
var rB = sq.B.replicate(false)
rA.pipe(rB).pipe(rA)

function viewQuery (sq) {
  var counter = {}, heads = {}
  var events = new EventEmitter
  return {
    map: function (msgs, next) {
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
          else feed.append({ key, n: (counter[key] || 0) + n }, cb)
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
