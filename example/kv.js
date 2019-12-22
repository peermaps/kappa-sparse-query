var level = require('level')
var { EventEmitter } = require('events')
var path = require('path')
var raf = require('random-access-file')

var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: { d: 'datadir', s: 'swarm' }
})

var db = level(path.join(argv.datadir,'db'))
var sub = require('subleveldown')

var sq = require('../')({
  db: sub(db,'sq'),
  valueEncoding: 'json',
  storage: p => {
    return raf(path.join(argv.datadir,p))
  }
})
var core = new(require('kappa-core'))()
var viewQuery = require('./kv/view-query.js')(sq, sub(db,'kv'))
sq.use('kv', viewQuery.query)
core.use('kv', sq.source(), viewQuery)

core.view.kv.events.on('result', function ({ key, value, id }) {
  console.log(`${key} => ${value} [${id}]`)
})

var swarm = require('discovery-swarm')()
swarm.on('connection', function (stream, info) {
  stream.pipe(sq.replicate(info.initiator)).pipe(stream)
})
swarm.join(argv.swarm)

var split = require('split2')
process.stdin.pipe(split()).on('data', function (buf) {
  var line = buf.toString(), m
  if (m = /^put (\S+) (\S+)(?:| (\S*))$/.exec(line)) {
    var doc = {
      key: m[1],
      value: m[2],
      links: (m[3] || '').split(',').filter(Boolean)
    }
    core.api.kv.put(doc, function (err, id) {
      if (err) console.error(err)
      else console.log(id)
    })
  } else if (m = /^open (\S+)$/.exec(line)) {
    core.api.kv.open(m[1].split(','))
  } else if (m = /^close (\S+)$/.exec(line)) {
    core.api.kv.close(m[1].split(','))
  }
})
