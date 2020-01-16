var test = require('tape')
var memdb = require('memdb')
var ram = require('random-access-memory')
var Kappa = require('kappa-core')
var { Readable } = require('stream')
var SQ = require('../')
var viewQuery = {
  kv: require('./viewquery/kv.js'),
  counter: require('./viewquery/counter.js')
}

test('combined', function (t) {
  t.plan(20)
  var sq = {
    A: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram }),
    B: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram })
  }
  var core = { A: new Kappa, B: new Kappa }
  var vq = {
    counter: {
      A: viewQuery.counter(sq.A),
      B: viewQuery.counter(sq.B)
    },
    kv: {
      A: viewQuery.kv(sq.A, memdb()),
      B: viewQuery.kv(sq.B, memdb())
    }
  }
  sq.A.use('kv', vq.kv.A.query)
  sq.B.use('kv', vq.kv.B.query)
  sq.A.use('counter', vq.counter.A.query)
  sq.B.use('counter', vq.counter.B.query)
  core.A.use('kv', sq.A.source('kv'), vq.kv.A)
  core.B.use('kv', sq.B.source('kv'), vq.kv.B)
  core.A.use('counter', sq.A.source('counter'), vq.counter.A)
  core.B.use('counter', sq.B.source('counter'), vq.counter.B)

  ;(function counter () {
    var results = { A: [], B: [] }
    var written = 0
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
  })()

  ;(function kv () {
    var results = { A: [], B: [] }
    var puts = []
    var pending = 2
    core.A.api.kv.events.on('result', function (result) {
      if (results.A.length > 0
      && result.id === results.A[results.A.length-1].id) return
      results.A.push(result)
      if (results.A.length === 1) {
        var doc = { type: 'kv', key: 'msg', value: 'ok...', links: [result.id] }
        core.A.api.kv.put(doc, function (err, id) {
          t.ifError(err)
          puts.push(id)
        })
      } else if (results.A.length === 3) {
        var doc = {
          type: 'kv',
          key: 'msg',
          value: 'merged',
          links: [results.A[1].id,results.A[2].id]
        }
        core.A.api.kv.put(doc, function (err, id) {
          t.ifError(err)
          puts.push(id)
        })
      } else if (results.A.length === 4) {
        if (--pending === 0) check()
      }
    })
    core.B.api.kv.events.on('result', function (result) {
      if (results.B.length > 0
      && result.id === results.B[results.B.length-1].id) return
      results.B.push(result)
      if (results.B.length === 2) {
        var doc = { type: 'kv', key: 'msg', value: 'fork', links: [results.B[0].id] }
        core.B.api.kv.put(doc, function (err, id) {
          t.ifError(err)
          puts.push(id)
        })
      } else if (results.B.length === 4) {
        if (--pending === 0) check()
      }
    })
    core.A.api.kv.open(['msg'])
    core.B.api.kv.open(['msg'])

    var r = {
      A: sq.A.replicate(true),
      B: sq.B.replicate(false)
    }
    r.A.pipe(r.B).pipe(r.A)

    var doc = { type: 'kv', key: 'msg', value: 'hi', links: [] }
    core.A.api.kv.put(doc, function (err, id) {
      t.ifError(err)
      puts.push(id)
    })

    function check () {
      core.A.api.kv.get('msg', function (err, ids) {
        t.ifError(err)
        t.deepEqual(ids, [results.A[3].id])
      })
      core.B.api.kv.get('msg', function (err, ids) {
        t.ifError(err)
        t.deepEqual(ids, [results.B[3].id])
      })
      t.deepEqual(results.A, results.B, 'A matches B')
      var expected = [
        {
          type: 'kv',
          key: 'msg',
          value: 'hi',
          id: puts[0],
          links: []
        },
        {
          type: 'kv',
          key: 'msg',
          value: 'ok...',
          id: puts[1],
          links: [puts[0]]
        },
        {
          type: 'kv',
          key: 'msg',
          value: 'fork',
          id: puts[2],
          links: [puts[0]]
        },
        {
          type: 'kv',
          key: 'msg',
          value: 'merged',
          id: puts[3],
          links: [puts[1],puts[2]]
        }
      ]
      t.deepEqual(results.A, expected)
    }
  })()
})
