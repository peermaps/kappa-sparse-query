var Kappa = require('kappa-core')
var Indexer = require('kappa-sparse-indexer')
var MStorage = require('multifeed-storage')
var Replicate = require('multifeed-replicate')
var { Writable } = require('readable-stream')
var { EventEmitter } = require('events')
var onend = require('end-of-stream')
var Query = require('hypercore-query-extension')
var Protocol = require('hypercore-protocol')

module.exports = Flow

function Flow (opts) {
  if (!(this instanceof Flow)) return new Flow(opts)
  this.feeds = opts.feeds || new MStorage(opts.storage)
  this._indexer = new Indexer({
    db: opts.db,
    name: opts.name || 'flow'
  })
  this._kcore = new Kappa
  this.api = this._kcore.view
  this._query = {}
  this._replicators = []
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
  var p = new Protocol(isInitiator, opts)
  var r = new Replicate(self.feeds, p)
  var h = { replicator: r, open: {} }
  self._replicators.push(h)
  var qs = {}
  onend(p, function () {
    var ix = self._replicators.indexOf(h)
    if (ix >= 0) self._replicators.splice(ix,1)
    Object.keys(qs).forEach(function (key) {
      self._query[key].close(qs[key])
    })
  })
  Object.keys(self._query).forEach(function (key) {
    var q = new Query({ api: self._query[key].api })
    p.registerExtension('query-' + key, q.extension())
    qs[key] = q
    self._query[key].open(q, new Writable({
      objectMode: true,
      write: function (row, enc, next) {
        var hkey = row.key.toString('hex')
        for (var i = 0; i < self._replicators.length; i++) {
          var r = self._replicators[i]
          if (!r.open[hkey]) {
            r.open[hkey] = true
            r.replicator.open(row.key)
          }
        }
        self._indexer.download(row.key, row.seq)
        next()
      }
    }))
  })
  return p
}
