var Kappa = require('kappa-core')
var Indexer = require('kappa-sparse-indexer')
var MStorage = require('multifeed-storage')
var Replicate = require('multifeed-replicate')
var { Writable } = require('readable-stream')
var { EventEmitter } = require('events')
var Query = require('hypercore-query-extension')
var Protocol = require('hypercore-protocol')
var pump = require('pump')

module.exports = Flow

function Flow (opts) {
  if (!(this instanceof Flow)) return new Flow(opts)
  if (!opts) opts = {}
  this.feeds = opts.feeds || new MStorage(opts.storage)
  this._indexer = new Indexer({
    db: opts.db,
    name: opts.name || 'flow'
  })
  this._kcore = new Kappa
  this.api = this._kcore.view
  this._query = {}
  this._added = {}
  this._getOpts = {}
  if (opts.valueEncoding) this._getOpts.valueEncoding = opts.valueEncoding
}
Flow.prototype = Object.create(EventEmitter.prototype)

Flow.prototype.use = function (name, view) {
  var v = Object.assign({}, view, { api: Object.assign({}, view.api) })
  // take out kappa-core instance from method args:
  Object.keys(v.api).forEach(function (key) {
    if (typeof v.api[key] !== 'function') return
    v.api[key] = function (kcore, ...args) {
      return view.api[key](...args)
    }
  })
  this._kcore.use(name, this._indexer.source(), v)
  this._query[name] = view.query
}

Flow.prototype.replicate = function (isInitiator, opts) {
  var self = this
  var p = new Protocol(isInitiator, { live: true, sparse: true })
  p.on('error', function (err) {})
  var r = new Replicate(self.feeds, p, { live: true, sparse: true })
  var open = {}
  Object.keys(self._query).forEach(function (key) {
    var q = new Query({ api: self._query[key].api })
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

Flow.prototype.addFeed = function (feed) {
  var self = this
  feed.ready(function () {
    var hkey = feed.key.toString('hex')
    if (self._added[hkey]) return
    self._added[hkey] = true
    self._indexer.add(feed)
  })
}

function has (obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key)
}
