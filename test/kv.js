var test = require('tape')
var memdb = require('memdb')
var ram = require('random-access-memory')
var Kappa = require('kappa-core')
var SQ = require('../')
var viewQuery = require('./viewquery/kv.js')

test('in memory key/value', function (t) {
  t.plan(10)
  var A = new SQ({
    db: memdb(),
    valueEncoding: 'json',
    storage: ram
  })
  var B = new SQ({
    db: memdb(),
    valueEncoding: 'json',
    storage: ram
  })
  var core = {
    A: new Kappa,
    B: new Kappa
  }
  var vq = {
    A: viewQuery(A, memdb()),
    B: viewQuery(B, memdb())
  }
  A.use('kv', vq.A.query)
  B.use('kv', vq.B.query)
  core.A.use('kv', A.source(), vq.A)
  core.B.use('kv', B.source(), vq.B)
  var results = { A: [], B: [] }
  var puts = []
  var pending = 2
  core.A.api.kv.events.on('result', function (result) {
    if (results.A.length > 0
    && result.id === results.A[results.A.length-1].id) return
    results.A.push(result)
    if (results.A.length === 1) {
      var doc = { key: 'msg', value: 'ok...', links: [result.id] }
      core.A.api.kv.put(doc, function (err, id) {
        t.ifError(err)
        puts.push(id)
      })
    } else if (results.A.length === 3) {
      var doc = {
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
      var doc = { key: 'msg', value: 'fork', links: [results.B[0].id] }
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
    A: A.replicate(true),
    B: B.replicate(false)
  }
  r.A.pipe(r.B).pipe(r.A)

  var doc = { key: 'msg', value: 'hi', links: [] }
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
    t.deepEqual(results.A, results.B)
    var expected = [
      {
        key: 'msg',
        value: 'hi',
        id: puts[0],
        links: []
      },
      {
        key: 'msg',
        value: 'ok...',
        id: puts[1],
        links: [puts[0]]
      },
      {
        key: 'msg',
        value: 'fork',
        id: puts[2],
        links: [puts[0]]
      },
      {
        key: 'msg',
        value: 'merged',
        id: puts[3],
        links: [puts[1],puts[2]]
      }
    ]
    t.deepEqual(results.A, expected)
  }
})
