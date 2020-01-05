var test = require('tape')
var tmpdir = require('os').tmpdir
var mkdirp = require('mkdirp')
var { randomBytes } = require('crypto')
var path = require('path')
var level = require('level')
var raf = require('random-access-file')
var Kappa = require('kappa-core')
var SQ = require('../')
var viewQuery = require('./viewquery/kv.js')

test('many-stage key/value', function (t) {
  t.plan(26)
  var queue = [
    [
      { feed: 'A', value: 'msg 0', links: [], seq: 0 },
      { feed: 'B', value: 'msg 1', links: ['A@0'], seq: 0 }
    ],
    [
      { feed: 'A', value: 'msg 2', links: ['B@0'], seq: 1 }
    ],
    [
      { feed: 'A', value: 'msg 3.1', links: ['A@1'], seq: 2 },
      { feed: 'B', value: 'msg 3.2', links: ['A@1'], seq: 1 },
      { feed: 'A', value: 'msg 4', links: ['A@2','B@1'], seq: 3 }
    ],
    [
      { feed: 'B', value: 'msg 5.1', links: ['A@3'], seq: 2 }
    ],
    [
      { feed: 'B', value: 'msg 5.2', links: ['A@3'], seq: 3 },
      { feed: 'A', value: 'msg 5.3', links: ['A@3'], seq: 4 },
      { feed: 'B', value: 'msg 6', links: ['B@2','B@3','A@4'], seq: 4 },
      { feed: 'A', value: 'msg 5.4', links: ['A@3'], seq: 5 },
    ],
    [
      { feed: 'B', value: 'msg 7', links: ['A@5','B@4'], seq: 5 }
    ]
  ]
  var expected = [
    { feed: 'A', value: 'msg 0', links: [], seq: 0 },
    { feed: 'B', value: 'msg 1', links: ['A@0'], seq: 0 },
    { feed: 'A', value: 'msg 2', links: ['B@0'], seq: 1 },
    { feed: 'A', value: 'msg 3.1', links: ['A@1'], seq: 2 },
    { feed: 'B', value: 'msg 3.2', links: ['A@1'], seq: 1 },
    { feed: 'A', value: 'msg 4', links: ['A@2','B@1'], seq: 3 },
    { feed: 'B', value: 'msg 5.1', links: ['A@3'], seq: 2 },
    { feed: 'B', value: 'msg 5.2', links: ['A@3'], seq: 3 },
    { feed: 'A', value: 'msg 5.3', links: ['A@3'], seq: 4 },
    { feed: 'B', value: 'msg 6', links: ['B@2','B@3','A@4'], seq: 4 },
    { feed: 'A', value: 'msg 5.4', links: ['A@3'], seq: 5 },
    { feed: 'B', value: 'msg 7', links: ['A@5','B@4'], seq: 5 }
  ]
  var results = { A: [], B: [] }
  var feedKeys = {}
  var dir = {
    A: path.join(tmpdir(), randomBytes(6).toString('hex')),
    B: path.join(tmpdir(), randomBytes(6).toString('hex'))
  }
  ;(function next () {
    if (queue.length === 0) return check()
    var feeds = {}
    var A = feeds.A = open(dir.A)
    var B = feeds.B = open(dir.B)
    A.core.api.kv.events.on('result', onresult('A'))
    B.core.api.kv.events.on('result', onresult('B'))
    var r = {
      A: A.sq.replicate(true),
      B: B.sq.replicate(false)
    }
    r.A.pipe(r.B).pipe(r.A)
    A.core.api.kv.open(['msg'])
    B.core.api.kv.open(['msg'])
    var fopts = { valueEncoding: 'json' }
    A.sq.feeds.getOrCreateLocal('default', fopts, function (err, feedA) {
      t.ifError(err)
      feedKeys.A = feedA.key.toString('hex')
      B.sq.feeds.getOrCreateLocal('default', fopts, function (err, feedB) {
        t.ifError(err)
        feedKeys.B = feedB.key.toString('hex')
        append()
      })
    })
    var lastSent = null, pending = { A: 0, B: 0 }
    var done = false
    function append () {
      if (queue.length === 0) return finish()
      if (queue[0].length === 0) {
        queue.shift()
        return finish()
      }
      if (queue.length === 0) return finish()
      var q = queue[0].shift()
      pending.A = 1
      pending.B = 1
      var doc = {
        key: 'msg',
        value: q.value,
        links: q.links.map(link => {
          var [k,seq] = link.split('@')
          return feedKeys[k] + '@' + seq
        })
      }
      lastSent = doc
      feeds[q.feed].core.api.kv.put(doc, function (err, id) {
        t.ifError(err)
      })
    }
    function finish () {
      done = true
      var p = 3
      close(A, function (err) {
        if (--p === 0) next()
      })
      close(B, function (err) {
        if (--p === 0) next()
      })
      if (--p === 0) next()
    }
    function onresult (key) {
      var okey = { A: 'B', B: 'A' }[key]
      return function (result) {
        if (done) return
        for (var i = 0; i < results[key].length; i++) {
          if (results[key][i].id === result.id) return
        }
        results[key].push(result)
        if (lastSent && result.value === lastSent.value) {
          pending[key] = 0
        }
        if (pending.A === 0 && pending.B === 0) {
          append()
        }
      }
    }
  })(0)
  function check () {
    t.deepEqual(results.A, results.B)
    t.deepEqual(results.A, expected.map(function (result) {
      return {
        id: feedKeys[result.feed] + '@' + result.seq,
        key: 'msg',
        value: result.value,
        links: result.links.map(link => {
          var [k,seq] = link.split('@')
          return feedKeys[k] + '@' + seq
        })
      }
    }))
  }
})

function open (dir) {
  mkdirp.sync(dir)
  var db = {
    sq: level(path.join(dir,'sq')),
    vq: level(path.join(dir,'vq'))
  }
  var sq = new SQ({
    db: db.sq,
    valueEncoding: 'json',
    storage: function (p) {
      return raf(path.join(dir,p))
    }
  })
  var core = new Kappa
  var vq = viewQuery(sq, db.vq)
  core.use('kv', sq.source(), vq)
  sq.use('kv', vq.query)
  return { sq, vq, db, core, dir }
}

function close (x, cb) {
  var pending = 4
  x.db.sq.close(function (err) {
    if (err) return cb(err)
    if (--pending === 0) cb()
  })
  x.core.api.kv.events.on('error', function () {})
  x.db.vq.close(function (err) {
    if (err) return cb(err)
    if (--pending === 0) cb()
  })
  x.sq.feeds.closeAll(function (err) {
    if (err) return cb(err)
    if (--pending === 0) cb()
  })
  if (--pending === 0) cb()
}
