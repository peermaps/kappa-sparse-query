var umkvps = require('unordered-materialized-kv-pubsub')
var { EventEmitter } = require('events')
var { Duplex } = require('readable-stream')
var onend = require('end-of-stream')

module.exports = function (sq, db) {
  var kv = umkvps(db)
  var events = new EventEmitter
  var session = kv.session(function (key, ids) {
    ids.forEach(function (id) {
      var [key,seq] = id.split('@')
      lookupMsg({ key, seq })
    })
  })
  var queries = []
  return {
    map: function (msgs, next) {
      msgs = msgs.filter(msg => msg.value && msg.value.type === 'kv')
      kv.batch(msgs.map(function (msg) {
        return {
          id: msg.key + '@' + msg.seq,
          key: msg.value.key,
          links: msg.value.links
        }
      }), next)
    },
    api: {
      events,
      open: function (core, keys) {
        session.open(keys)
        queries.forEach(function (q) {
          q.write(JSON.stringify(['open',keys])+'\n')
        })
      },
      close: function (core, keys) {
        session.close(keys)
        queries.forEach(function (q) {
          q.write(JSON.stringify(['close',keys])+'\n')
        })
      },
      get: function (core, key, cb) {
        kv.get(key, cb)
      },
      put: function (core, doc, cb) {
        doc.type = 'kv'
        sq.feeds.getOrCreateLocal('default', { valueEncoding: 'json' }, onfeed)
        function onfeed (err, feed) {
          if (err) return cb(err)
          feed.append(doc, function (err, seq) {
            if (err) cb(err)
            else cb(null, feed.key.toString('hex') + '@' + seq)
          })
        }
      }
    },
    query: {
      api: { open },
      replicate: function ({ query, protocol }) {
        var arg = Buffer.from(JSON.stringify(session.getOpenKeys()))
        var q = query('open', arg)
        queries.push(q)
        onend(protocol, function () {
          var ix = queries.indexOf(q)
          if (ix >= 0) queries.splice(ix,1)
        })
      }
    }
  }
  function open (buf) {
    var session = kv.session(function (k, ids) {
      ids.forEach(function (id) {
        var [key,seq] = id.split('@')
        key = Buffer.from(key,'hex')
        stream.push({ key, seq })
      })
    })
    session.open(JSON.parse(buf.toString()))
    var stream = new Duplex({
      readableObjectMode: true,
      read: function () {},
      write: function (buf, enc, next) {
        try { var msg = JSON.parse(buf.toString()) }
        catch (err) { return next() }
        if (msg[0] === 'open') {
          session.open(msg.slice(1))
        } else if (msg[0] === 'close') {
          session.close(msg.slice(1))
        }
        next()
      },
      destroy: cleanup,
      final: function (next) {
        cleanup()
        next()
      }
    })
    queries.push(stream)
    return stream
    function cleanup () {
      session.destroy()
      var ix = queries.indexOf(stream)
      if (ix >= 0) queries.splice(ix,1)
    }
  }
  function lookupMsg ({ key, seq }) {
    sq.feeds.get(key, { valueEncoding: 'json' }, function (err, feed) {
      if (err) return events.emit('error', err)
      feed.get(seq, function (err, doc) {
        if (err) return events.emit('error', err)
        doc.id = key + '@' + seq
        events.emit('result', doc)
      })
    })
  }
}
