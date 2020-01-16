var Indexer = require('kappa-sparse-indexer')
var MStorage = require('multifeed-storage')
var Replicate = require('multifeed-replicate')
var { Writable } = require('readable-stream')
var { EventEmitter } = require('events')
var { nextTick } = process
var Query = require('hypercore-query-extension')
var Protocol = require('hypercore-protocol')
var pump = require('pump')
var onend = require('end-of-stream')
var sub = require('subleveldown')

module.exports = SQ

function SQ (opts) {
  var self = this
  if (!(self instanceof SQ)) return new SQ(opts)
  if (!opts) opts = {}
  self._db = opts.db
  self.feeds = opts.feeds || new MStorage(opts.storage)
  self.feeds.on('create-local', function (feed) {
    self._addFeed(feed)
  })
  self.feeds.on('create-remote', function (feed) {
    self._addFeed(feed)
  })
  self.feeds.on('open', function (feed) {
    self._addFeed(feed)
  })
  self._indexers = {}
  self._query = {}
  self._added = {}
  self._getOpts = {}
  if (opts.valueEncoding) self._getOpts.valueEncoding = opts.valueEncoding
}
SQ.prototype = Object.create(EventEmitter.prototype)

SQ.prototype._getIndexer = function (type) {
  var self = this
  if (!self._indexers.hasOwnProperty(type)) {
    self._indexers[type] = new Indexer({
      db: sub(self._db, type),
      name: type,
      loadValue: function (key, seq, next) {
        self.feeds.get(key, self._getOpts, function (err, feed) {
          if (err) return next(err)
          feed.get(seq, self._getOpts, function (err, value) {
            if (err) next(err)
            else next(null, { key, seq, value })
          })
        })
      }
    })
  }
  return self._indexers[type]
}

SQ.prototype.use = function (name, query) {
  if (!query || typeof query !== 'object') {
    throw new Error('query must be an object')
  }
  this._query[name] = query
}

SQ.prototype.source = function (type) {
  return this._getIndexer(type).source()
}

SQ.prototype.replicate = function (isInitiator, opts) {
  var self = this
  if (!opts) opts = {}
  var p = isInitiator && typeof isInitiator === 'object'
    ? isInitiator
    : opts.stream || new Protocol(isInitiator, { live: true, sparse: true })
  p.on('error', function (err) {})
  var r = new Replicate(self.feeds, p, { live: true, sparse: true })
  var open = {}, streams = []
  onend(p, function (err) {
    for (var i = 0; i < streams.length; i++) {
      if (typeof streams[i].close === 'function') {
        s.close()
      }
    }
  })
  Object.keys(self._query).forEach(function (type) {
    var q = new Query({ api: self._query[type].api || {} })
    p.registerExtension('query-' + type, q.extension())
    if (typeof self._query[type].replicate === 'function') {
      self._query[type].replicate({ query, protocol: p, replicate: r })
    }
    function query (name, arg) {
      var s = q.query(name, arg)
      streams.push(s)
      onend(s, function () {
        var ix = streams.indexOf(s)
        if (ix >= 0) streams.splice(s,1)
      })
      pump(s, new Writable({
        objectMode: true,
        write: function (row, enc, next) {
          if (Buffer.isBuffer(row.key)) {
            var key = row.key
            var hkey = row.key.toString('hex')
          } else {
            var hkey = row.key
            var key = Buffer.from(hkey,'hex')
          }
          if (has(self._added, hkey)) {
            download()
          } else {
            self.feeds.getOrCreateRemote(key, self._getOpts, onfeed)
          }
          function onfeed (err, feed) {
            if (err) self.emit('error', err)
            else download()
          }
          function download () {
            if (!has(open,hkey)) {
              open[hkey] = true
              r.open(key, { live: true, sparse: true })
            }
            self._getIndexer(type).download(key, row.seq)
            next()
          }
        }
      }))
      return s
    }
  })
  return p
}

SQ.prototype._addFeed = function (feed) {
  var self = this
  // keys are assumed to be ready
  var hkey = feed.key.toString('hex')
  if (self._added[hkey]) return
  self._added[hkey] = true
  Object.keys(self._indexers).forEach(function (type) {
    self._indexers[type].addReady(feed)
  })
}

function has (obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key)
}

function noop () {}
