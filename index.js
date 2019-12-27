var Indexer = require('kappa-sparse-indexer')
var MStorage = require('multifeed-storage')
var Replicate = require('multifeed-replicate')
var { Writable } = require('readable-stream')
var { EventEmitter } = require('events')
var Query = require('hypercore-query-extension')
var Protocol = require('hypercore-protocol')
var pump = require('pump')

module.exports = SQ

function SQ (opts) {
  if (!(this instanceof SQ)) return new SQ(opts)
  if (!opts) opts = {}
  this.feeds = opts.feeds || new MStorage(opts.storage)
  this._indexer = new Indexer({
    loadValue: false,
    db: opts.db,
    name: opts.name || 'flow'
  })
  this._query = {}
  this._added = {}
  this._getOpts = {}
  if (opts.valueEncoding) this._getOpts.valueEncoding = opts.valueEncoding
}
SQ.prototype = Object.create(EventEmitter.prototype)

SQ.prototype.use = function (name, query) {
  if (!query || typeof query !== 'object') {
    throw new Error('query must be an object')
  }
  this._query[name] = query
}

SQ.prototype.source = function () {
  return this._indexer.source()
}

SQ.prototype.replicate = function (isInitiator, opts) {
  var self = this
  if (!opts) opts = {}
  var p = isInitiator && typeof isInitiator === 'object'
    ? isInitiator
    : opts.stream || new Protocol(isInitiator, { live: true, sparse: true })
  p.on('error', function (err) {})
  var r = new Replicate(self.feeds, p, { live: true, sparse: true })
  var open = {}
  Object.keys(self._query).forEach(function (key) {
    var q = new Query({ api: self._query[key].api || {} })
    p.registerExtension('query-' + key, q.extension())
    if (typeof self._query[key].replicate === 'function') {
      self._query[key].replicate({ query, protocol: p, replicate: r })
    }
    function query (name, arg) {
      var s = q.query(name, arg)
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
            self._indexer.download(key, row.seq)
          } else {
            self.feeds.getOrCreateRemote(key, self._getOpts, onfeed)
          }
          function onfeed (err, feed) {
            if (err) return self.emit('error', err)
            self._added[hkey] = true
            self._indexer.add(feed, function () {
              self._indexer.download(key, row.seq)
              openFeed(key, hkey)
            })
          }
          next()
        }
      }))
      return s
    }
    function openFeed (key, hkey) {
      if (has(open,hkey)) return
      open[hkey] = true
      r.open(key, { live: true, sparse: true })
    }
  })
  return p
}

SQ.prototype.addFeed = function (feed, cb) {
  var self = this
  if (!cb) cb = noop
  feed.ready(function () {
    var hkey = feed.key.toString('hex')
    if (self._added[hkey]) return cb()
    self._added[hkey] = true
    self._indexer.add(feed, cb)
  })
}

function has (obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key)
}

function noop () {}
