var test = require('tape')
var tmpdir = require('os').tmpdir
var mkdirp = require('mkdirp')
var { randomBytes } = require('crypto')
var path = require('path')
var level = require('level')
var raf = require('random-access-file')
var Kappa = require('kappa-core')
var sub = require('subleveldown')
var SQ = require('../')
var viewQuery = require('./viewquery/kv.js')

test('multi-stage key/value', function (t) {
  t.plan(24)
  var A = open(path.join(tmpdir(), randomBytes(6).toString('hex')))
  var B = open(path.join(tmpdir(), randomBytes(6).toString('hex')))
  var results = { A: [], B: [] }
  var puts = []
  var pending = 2
  function sendOne () {
    var doc = { key: 'msg', value: 'ok...', links: [results.A[0].id] }
    A.core.api.kv.put(doc, function (err, id) {
      t.ifError(err)
      puts.push(id)
    })
  }
  A.core.api.kv.events.on('result', function (result) {
    if (results.A.length > 0
    && result.id === results.A[results.A.length-1].id) return
    results.A.push(result)
    if (results.A.length === 1 && results.B.length === 1) {
      sendOne()
    } else if (results.A.length === 3) {
      var doc = {
        key: 'msg',
        value: 'merged',
        links: [results.A[1].id,results.A[2].id]
      }
      A.core.api.kv.put(doc, function (err, id) {
        t.ifError(err)
        puts.push(id)
      })
    } else if (results.A.length === 4) {
      if (--pending === 0) check()
    }
  })
  B.core.api.kv.events.on('result', function (result) {
    if (results.B.length > 0
    && result.id === results.B[results.B.length-1].id) return
    results.B.push(result)
    if (results.A.length === 1 && results.B.length === 1) {
      sendOne()
    } else if (results.B.length === 2) {
      var doc = { key: 'msg', value: 'fork', links: [results.B[0].id] }
      B.core.api.kv.put(doc, function (err, id) {
        t.ifError(err)
        puts.push(id)
      })
    } else if (results.B.length === 4) {
      if (--pending === 0) check()
    }
  })
  A.core.api.kv.open(['msg'])
  B.core.api.kv.open(['msg'])

  var r = {
    A: A.sq.replicate(true),
    B: B.sq.replicate(false)
  }
  r.A.pipe(r.B).pipe(r.A)

  var doc = { key: 'msg', value: 'hi', links: [] }
  A.core.api.kv.put(doc, function (err, id) {
    t.ifError(err)
    puts.push(id)
  })

  function check () {
    var pending = 3
    A.core.api.kv.get('msg', function (err, ids) {
      t.ifError(err)
      t.deepEqual(ids, [results.A[3].id])
      if (--pending == 0) closeAndReopen()
    })
    B.core.api.kv.get('msg', function (err, ids) {
      t.ifError(err)
      t.deepEqual(ids, [results.B[3].id])
      if (--pending == 0) closeAndReopen()
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
    if (--pending == 0) closeAndReopen()
  }
  function closeAndReopen () {
    var pending = 7
    A.db.sq.close(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    A.db.vq.close(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    A.sq.feeds.closeAll(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    B.db.sq.close(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    B.db.vq.close(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    B.sq.feeds.closeAll(function (err) {
      t.ifError(err)
      if (--pending === 0) done()
    })
    if (--pending === 0) done()
    function done () {
      A = open(A.dir)
      B = open(B.dir)
      phaseTwo()
    }
  }
  function phaseTwo () {
    var results = { A: [], B: [] }
    A.core.api.kv.events.on('result', function (result) {
      console.log('RESULT:A', result)
      if (results.A.length > 0
      && result.id === results.A[results.A.length-1].id) return
      results.A.push(result)
      if (results.A.length === 1 && results.B.length === 1) writeOne()
      if (results.A.length === 2 && results.B.length === 2) writeTwo()
      if (results.A.length === 3 && results.B.length === 3) checkTwo()
    })
    B.core.api.kv.events.on('result', function (result) {
      console.log('RESULT:B', result)
      if (results.B.length > 0
      && result.id === results.B[results.B.length-1].id) return
      results.B.push(result)
      if (results.A.length === 1 && results.B.length === 1) writeOne()
      if (results.A.length === 2 && results.B.length === 2) writeTwo()
      if (results.A.length === 3 && results.B.length === 3) checkTwo()
    })
    var r = {
      A: A.sq.replicate(true),
      B: B.sq.replicate(false)
    }
    r.A.pipe(r.B).pipe(r.A)
    A.core.api.kv.open(['msg'])
    B.core.api.kv.open(['msg'])
    function writeOne () {
      var doc = { key: 'msg', value: 'skelly', links: [puts[puts.length-1]] }
      A.core.api.kv.put(doc, function (err, id) {
        t.ifError(err)
        puts.push(id)
      })
    }
    function writeTwo () {
      var doc = { key: 'msg', value: 'skellington', links: [puts[puts.length-1]] }
      B.core.api.kv.put(doc, function (err, id) {
        t.ifError(err)
        puts.push(id)
      })
    }
  }
  function checkTwo () {
    A.core.api.kv.get('msg', function (err, ids) {
      t.ifError(err)
      t.deepEqual(ids, [results.A[2].id])
    })
    B.core.api.kv.get('msg', function (err, ids) {
      t.ifError(err)
      t.deepEqual(ids, [results.B[2].id])
    })
    t.deepEqual(results.A, results.B)
    var expected = [
      {
        key: 'msg',
        value: 'merged',
        id: puts[3],
        links: [puts[1],puts[2]]
      },
      {
        key: 'msg',
        value: 'skelly',
        id: puts[4],
        links: [puts[3]]
      },
      {
        key: 'msg',
        value: 'skellington',
        id: puts[5],
        links: [puts[4]]
      }
    ]
    t.deepEqual(results.A, expected)
  }
})

function open (dir) {
  mkdirp.sync(dir)
  var db = {
    sq: level(path.join(dir,'sq')),
    vq: level(path.join(dir,'vq'))
  }
  var stores = {}
  var sq = new SQ({
    db: db.sq,
    valueEncoding: 'json',
    storage: function (p) {
      stores[p] = stores[p] || raf(path.join(dir,p))
      return stores[p]
    }
  })
  var core = new Kappa
  var vq = viewQuery(sq, db.vq)
  core.use('kv', sq.source(), vq)
  sq.use('kv', vq.query)
  return { sq, vq, db, stores, core, dir }
}
