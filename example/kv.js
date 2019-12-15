var umkvl = require('unordered-materialized-kv-live')
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
var flow = require('../')({
  db: sub(db,'flow'),
  storage: p => raf(path.join(argv.datadir,p))
})

flow.use('kv', require('./kv/view-query.js')(flow, sub(db,'kv')))
flow.api.kv.events.on('result', function ({ key, value }) {
  console.log(`${key} => ${value}`)
})

var swarm = require('discovery-swarm')()
swarm.on('connection', function (stream, info) {
  stream.pipe(flow.replicate(info.initiator)).pipe(stream)
})
swarm.join(argv.swarm)

var split = require('split2')
process.stdin.pipe(split()).on('data', function (buf) {
  var line = buf.toString(), m
  if (m = /^put (\S+) (\S+)(?:| (\S*))$/.exec(line)) {
    var doc = { key: m[1], value: m[2], links: (m[3] || '').split(',') }
    flow.api.kv.put(doc, function (err, id) {
      if (err) console.error(err)
      else console.log(id)
    })
  } else if (m = /^open (\S+)$/.exec(line)) {
    flow.api.kv.open(m[1].split(','))
  } else if (m = /^close (\S+)$/.exec(line)) {
    flow.api.kv.close(m[1].split(','))
  }
})
