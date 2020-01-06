var test = require('tape')
var memdb = require('memdb')
var ram = require('random-access-memory')
var { EventEmitter } = require('events')
var { Readable } = require('stream')
var SQ = require('../')

test('counters', function (t) {
  t.plan(10)
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

  var results = { A: [], B: [] }
  core.A.view.counter.events .on('counter', (key,n) => {
    results.A.push({key,n})
    if (results.A.length === written && results.B.length === written) write()
  })
  core.B.view.counter.events .on('counter', (key,n) => {
    results.B.push({key,n})
    if (results.A.length === written && results.B.length === written) write()
  })

  var rA = sq.A.replicate(true)
  var rB = sq.B.replicate(false)
  rA.pipe(rB).pipe(rA)

  var writes = [
    { core: 'A', key: 'e', n: 7 },
    { core: 'A', key: 'a', n: 3 },
    { core: 'B', key: 'c', n: 1 },
    { core: 'A', key: 'b', n: 5 },
    { core: 'B', key: 'a', n: 6 },
    { core: 'B', key: 'e', n: 10 },
    { core: 'B', key: 'e', n: 11 },
    { core: 'A', key: 'd', n: 2 }
  ]
  var written = 0
  process.nextTick(write)

  function write () {
    if (written === writes.length) return check()
    var w = writes[written++]
    if (!w) return
    core[w.core].view.counter.add(w.key, w.n, function (err) {
      t.ifError(err)
    })
  }
  function check () {
    t.deepEqual(results.A, results.B, 'A matches B')
    t.deepEqual(results.A, [
      { key: 'e', n: 7 },
      { key: 'a', n: 3 },
      { key: 'c', n: 1 },
      { key: 'b', n: 5 },
      { key: 'a', n: 9 },
      { key: 'e', n: 17 },
      { key: 'e', n: 28 },
      { key: 'd', n: 2 }
    ])
  }
})

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
