var umkvl = require('unordered-materialized-kv-live')
var { EventEmitter } = require('events')
var { Duplex } = require('readable-stream')

module.exports = function (flow, db) {
  var kv = umkvl(db)
  var refs = {}, queries = {}
  kv.on('value', function (k, ids) {
    ids.forEach(function (id) {
      var [key,seq] = id.split('@')
      ;(queries[k] || []).forEach(function (q) {
        q.push({ key, seq })
      })
    })
  })
  var apiQ = { push: lookupMsg }
  var events = new EventEmitter
  var subs = {}
  var openQueries = []

  return {
    map: function (msgs, next) {
      kv.batch(msgs.map(msg => msg.value), next)
    },
    api: {
      events,
      put: function (doc, cb) {
        flow.feeds.getOrCreateLocal('default', { valueEncoding: 'json' }, onfeed)
        function onfeed (err, feed) {
          if (err) return cb(err)
          feed.append(doc, function (err, seq) {
            if (err) cb(err)
            else cb(null, feed.key.toString('hex') + '@' + seq)
          })
        }
      },
      open: function (keys) {
        if (!Array.isArray(keys)) keys = [keys]
        openQueries.forEach(function (q) {
          q.write(Buffer.from(JSON.stringify(['o'].concat(keys))))
        })
        keys.forEach(function (key) {
          openKey(key)
          subs[key] = true
          if (!queries[key]) queries[key] = apiQ
          else queries[key].push(apiQ)
        })
      },
      close: function (keys) {
        if (!Array.isArray(keys)) keys = [keys]
        openQueries.forEach(function (q) {
          q.write(Buffer.from(JSON.stringify(['c'].concat(keys))))
        })
        keys.forEach(function (key) {
          delete subs[key]
          if (--refs[key] === 0) {
            kv.close(key)
            delete refs[key]
          }
          var ix = queries[key].indexOf(apiQ)
          if (ix >= 0) queries[key].splice(ix,1)
          if (queries[key].length === 0) delete queries[key]
        })
      }
    },
    query: {
      open: function (q, w) {
        var arg = Buffer.from(JSON.stringify(Object.keys(subs)))
        var r = q.query('open', arg)
        openQueries.push(r)
        r.pipe(w)
      },
      close: function (q) {
        console.log('close...')
      },
      api: { open: openQuery }
    }
  }
  function openQuery (buf) {
    var keys = JSON.parse(buf)
    keys.forEach(openQueryKey)
    var stream = new Duplex({
      readableObjectMode: true,
      read: function () {},
      write: function (buf, enc, next) {
        try { var msg = JSON.parse(buf) }
        catch (err) { return next() }
        if (msg[0] === 'o') {
          msg = msg.slice(1)
          keys = keys.concat(msg)
          msg.forEach(openQueryKey)
        } else if (msg[0] === 'c') {
          msg = msg.slice(1)
          keys = keys.filter(function (key) {
            return msg.indexOf(key) >= 0
          })
          msg.forEach(closeQueryKey)
        }
        next()
      },
      destroy: cleanup,
      final: function (next) {
        cleanup()
        next()
      }
    })
    return stream
    function cleanup () {
      keys.forEach(function (key) {
        if (--refs[key] === 0) {
          kv.close(key)
          delete refs[key]
        }
        var ix = queries[key].indexOf(stream)
        if (ix >= 0) queries[key].splice(ix,1)
        if (queries[key].length === 0) delete queries[key]
      })
    }
    function openQueryKey (key) {
      openKey(key)
      if (!queries[key]) queries[key] = [stream]
      else queries[key].push(stream)
    }
    function closeQueryKey (key) {
      console.log('TODO: closeQueryKey')
    }
  }
  function openKey (key) {
    if (!refs[key]) {
      kv.open(key)
      refs[key] = 0
    }
    refs[key]++
  }
  function lookupMsg ({ key, seq }) {
    flow.feeds.get(key, { valueEncoding: 'json' }, function (err, feed) {
      if (err) return events.emit('error', err)
      feed.get(seq, function (err, doc) {
        if (err) return events.emit('error', err)
        events.emit('result', doc)
      })
    })
  }
}
