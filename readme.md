# kappa-sparse-query

replicate feeds for kappa-core@experimental views using sparse queries

this module combines:

* [multifeed-storage][]
* [multifeed-replicate][]
* [hypercore-query-extension][]
* [kappa-sparse-replicator][]

[multifeed-storage]: https://github.com/kappa-db/multifeed-storage
[multifeed-replicate]: https://github.com/kappa-db/multifeed-replicate
[hypercore-query-extension]: https://github.com/peermaps/hypercore-query-extension
[kappa-sparse-replicator]: https://github.com/Frando/kappa-sparse-indexer

# example

This example is a bit long, but it's about the simplest you can get with this
kind of thing. The example sets up 2 kappa-cores and each core is associated
with a kappa-sparse-query to drive downloads of a very simple state-based
counter crdt which writes a random increment value to a random key out of `'a'`,
`'b'`, `'c', `'d'`, and `'e'` to either feed A or feed B, at random. A write to
one feed gets processed and propagated through the query side-channel and the
other feed downloads the relevant message, updating its own counter view such
that both feeds are synchronized.

``` js
var memdb = require('memdb')
var ram = require('random-access-memory')
var { EventEmitter } = require('events')
var { Readable } = require('stream')

var SQ = require('kappa-sparse-query')
var sq = {
  A: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram }),
  B: new SQ({ db: memdb(), valueEncoding: 'json', storage: ram })
}
var Kappa = require('kappa-core')
var core = { A: new Kappa, B: new Kappa }
var vq = { A: viewQuery(sq.A), B: viewQuery(sq.B) }
sq.A.use('counter', vq.A.query)
sq.B.use('counter', vq.B.query)
core.A.use('counter', sq.A.source('counter'), vq.A)
core.B.use('counter', sq.B.source('counter'), vq.B)

core.A.view.counter.events
  .on('counter', (key,n) => console.log(`A:${key}=${n}`))
core.B.view.counter.events
  .on('counter', (key,n) => console.log(`B:${key}=${n}`))

process.nextTick(function f () {
  var key = String.fromCharCode(97 + Math.floor(Math.random()*5))
  var n = Math.floor(Math.random()*10)
  if (Math.random() > 0.5) {
    core.A.view.counter.add(key, n, next)
  } else {
    core.B.view.counter.add(key, n, next)
  }
  function next (err) {
    if (err) console.error(err)
    else setTimeout(f, 1000)
  }
})

var rA = sq.A.replicate(true)
var rB = sq.B.replicate(false)
rA.pipe(rB).pipe(rA)

function viewQuery (sq) {
  var counter = {}, heads = {}
  var events = new EventEmitter
  return {
    map: function (msgs, next) {
      msgs.forEach((msg) => {
        if (msg.value.n > (counter[msg.value.key] || 0)) {
          heads[msg.value.key] = { key: msg.key, seq: msg.seq }
          counter[msg.value.key] = msg.value.n
          events.emit('counter', msg.value.key, counter[msg.value.key])
        }
      })
      next()
    },
    api: {
      events,
      get (key) { return counter[key] },
      add (kcore, key, n, cb) {
        sq.feeds.getOrCreateLocal('default', { valueEncoding: 'json' }, onfeed)
        function onfeed (err, feed) {
          if (err) return cb(err)
          else feed.append({ key, n: (counter[key] || 0) + n }, cb)
        }
      }
    },
    query: {
      api: {
        counter: function () {
          var r = new Readable({ objectMode: true, read: function () {} })
          r.close = function () {
            events.removeListener('counter', oncount)
          }
          Object.keys(heads).forEach(fkey => r.push(heads[fkey]))
          events.on('counter', oncount)
          return r
          function oncount (key, n) { r.push(heads[key]) }
        }
      },
      replicate: function ({ query }) {
        query('counter')
      }
    }
  }
}
```

output:

```
$ node example/counters.js 
A:e=2
B:e=2
B:c=7
A:c=7
B:d=6
A:d=6
B:c=12
A:c=12
A:b=9
B:b=9
B:a=8
A:a=8
B:d=15
A:d=15
B:b=18
A:b=18
^C
```

For a more useful but longer example, check out the sparse key/value
implementation in `example/kv.js` and `example/kv/view-query.js`.

# api

``` js
var SQ = require('kappa-sparse-query')
```

## var sq = new SQ(opts)

Create a new kappa-sparse-query instance from:

* `opts.db` - leveldb instance
* `opts.valueEncoding` - open feeds with this valueEncoding (default: 'binary')
* `opts.storage` - random access storage instance
  (or you can supply a [multifeed-storage][] instance as `opts.feed` instead)

## sq.use(name, query)

Create a query namespace under a string `name` (like kappa-core views with
its `.use()`) from:

* `query.api` - object mapping queries to functions returning objectMode
  streams. see the [hypercore-query-extension][] for more details on the format
  of the objects, but each object should have a feed `key` and `seq`.
* `query.replicate({ query, protocol, replicate })` - optional function that
  gets called during replication with a `query(name, args)` function (see
  [hypercore-query-extension][]), the hypercore `protocol` instance, and the
  [multifeed-replicate][] instance. Use this function to do session setup and
  teardown by listening for when the protocol stream ends (use
  [end-of-stream][]).

The `query()` function in the replicate handler is used to call the query api on
the other end.

[end-of-stream]: https://github.com/mafintosh/end-of-stream

## sq.source(name)

Create a source for kappa-core's `use()` method for any views with access
patterns driven by this module. If you call `sq.source(name)` again with the
same name, you will get back the same source instance.

## var stream = sq.replicate(isInitiator, opts)

Create a replication `stream` to synchronize feeds based on query results.

## sq.feeds

[multifeed-storage][] instance to open or lookup feeds

# install

npm install kappa-sparse-query

# license

bsd
