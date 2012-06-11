redis = require 'redis'
redisInfo = require './redisInfo'
transaction = Serializer = Promise = null

exports = module.exports = (racer) ->
  transaction = racer.protected.transaction
  Serializer = racer.protected.Serializer
  Promise = racer.util.Promise
  racer.registerAdapter 'journal', 'Redis', JournalRedis

exports.useWith = server: true, browser: false
exports.decorate = 'racer'

exports.JournalRedis = JournalRedis = (options) ->
  {port, host, db, password} = options
  # Client for data access and event publishing
  @_redisClient = redisClient = redis.createClient port, host, options
  # Client for internal Racer event subscriptions
  @_subClient = subClient = redis.createClient port, host, options

  if password
    throwOnErr = (err) -> throw err if err
    redisClient.auth password, throwOnErr
    subClient.auth password, throwOnErr

  @_startIdPromise = startIdPromise = new Promise

  @_lockQueue = {}

  # TODO: Make sure there are no weird race conditions here, since we are
  # caching the value of starts and it could potentially be stale when a
  # transaction is received
  # TODO: Make sure this works when redis crashes and is restarted
  redisStarts = null
  ignoreSubscribe = false
  do subscribeToStarts = (db) ->
    return ignoreSubscribe = false if ignoreSubscribe

    # Calling select right away queues the command before any commands that
    # a client might add before connect happens. If select is not queued first,
    # the subsequent commands could happen on the wrong db
    if db isnt undefined
      return redisClient.select db, (err) ->
        throw err if err
        subscribeToStarts()

    redisInfo.subscribeToStarts subClient, redisClient, (err, starts) ->
      throw err if err
      redisStarts = starts
      {0: firstStart} = starts
      [startId] = firstStart
      startIdPromise.clear().resolve null, startId

  # Ignore the first connect event
  ignoreSubscribe = true
  redisClient.on 'connect', subscribeToStarts
  redisClient.on 'end', ->
    redisStarts = null
    startIdPromise.clear()

  return

JournalRedis::=
  flush: (cb) ->
    redisClient = @_redisClient
    startIdPromise = @_startIdPromise
    # TODO Be more granular about this. Remove ind keys instead of flushdb
    redisClient.flushdb (err) ->
      return cb err if err
      redisInfo.onStart redisClient, (err) ->
        return cb err if err
        startIdPromise.clear()
        cb null

  disconnect: ->
    @_redisClient.end()
    @_subClient.end()

  startId: (cb) ->
    @_startIdPromise.on cb

  version: (cb) ->
    @_redisClient.get 'ver', (err, ver) ->
      return cb err if err
      cb null, parseInt(ver, 10)

  txnsSince: (ver, clientId, pubSub, cb) ->
    return cb null, []  unless pubSub.hasSubscriptions clientId

    # TODO Replace with a LUA script that does filtering?
    @_redisClient.zrangebyscore 'txns', ver, '+inf', 'withscores', (err, vals) ->
      return cb err  if err
      txn = null
      txns = []
      for val, i in vals
        if i % 2
          continue unless pubSub.subscribedTo clientId, transaction.getPath(txn)
          transaction.setVer txn, +val
          txns.push txn
        else
          txn = JSON.parse val
      cb null, txns

  eachTxnSince: (ver, opts) ->
    redisClient = @_redisClient
    {meta: {txn}, each, done} = opts
    # If the ver of a transaction is null or undefined, pass an empty string
    # for sinceVer, which indicates not to return a journal. Thus, no conflicts
    # will be found
    sinceVer = if `ver == null` then '' else ver + 1
    if transaction.isCompound txn
      paths = (transaction.op.getPath op for op in transaction.ops txn)
    else
      paths = [transaction.getPath txn]

    locks = []
    for path in paths
      for lock in getLocks path
        locks.push lock  if locks.indexOf(lock) == -1

    @_lock locks, sinceVer, paths, MAX_RETRIES, RETRY_DELAY, (err, numLocks, lockVal, txns) =>
      path = paths[0]

      return done null, {numLocks, locks, lockVal} unless i = txns?.length

      next = (err) ->
        if err
          if err == 'conflict' || err == 'duplicate'
            return redisClient.eval UNLOCK, numLocks, locks..., lockVal, (unlockErr) =>
              return done(if unlockErr then unlockErr else err)
          else
            return done err
        if txn = txns[--i]
          return each null, JSON.parse(txn), next
        return done null, {numLocks, locks, lockVal}

      next()

  add: (txn, {numLocks, locks, lockVal}, cb) ->
    # Commit if there are no conflicts and the locks are still held
    @_redisClient.eval LOCKED_COMMIT, numLocks, locks..., lockVal, JSON.stringify(txn), (err, ver) =>
      return cb err if err
      return cb 'lockReleased' if ver is 0
      cb null, ver

      path = transaction.getPath txn
      # If another transaction failed to lock because of this transaction,
      # shift it from the queue
      @_tryNextLock path

  _tryNextLock: (path) ->
    return unless queue = @_lockQueue[path]
    if args = queue.shift()
      unless queue.length
        delete @_lockQueue[path]
      @_lock args...

  _lock: (locks, sinceVer, path, retries, delay, cb) ->
    # Callback has signature: fn(err, lockVal, txns)
    numKeys = locks.length
    @_redisClient.eval LOCK, numKeys, locks..., sinceVer, (err, [lockVal, txns]) =>
      return cb err if err
      if lockVal
        return cb null, numKeys, lockVal, txns
      if retries
        queue = @_lockQueue[path] ||= []
        # Maintain a queue so that if this lock conflicts with another operation
        # on the same server and the same path, the lock can be retried immediately
        queue.push [locks, sinceVer, path, retries - 1, delay * 2, cb]
        # Use an exponential timeout in case the conflict is because of a lock
        # on a child path or is coming from a different server
        return setTimeout =>
          @_tryNextLock path
          @_lock args... if args = queue.shift()
        , delay
      return cb 'lockMaxRetries', numLocks

# Example output:
# getLocks("a.b.c") => [".a.b.c", ".a.b", ".a"]
JournalRedis.getLocks = getLocks = (path) ->
  lockPath = ''
  return (lockPath += '.' + segment for segment in path.split '.').reverse()

JournalRedis.MAX_RETRIES = MAX_RETRIES = 10
# Initial delay in milliseconds. Exponentially increases
JournalRedis.RETRY_DELAY = RETRY_DELAY = 5

# Lock timeout in seconds. Could be +/- one second
JournalRedis.LOCK_TIMEOUT = LOCK_TIMEOUT = 3
# Use 32 bits for timeout
JournalRedis.LOCK_TIMEOUT_MASK = LOCK_TIMEOUT_MASK = 0x100000000
# Use 20 bits for lock clock
JournalRedis.LOCK_CLOCK_MASK = LOCK_CLOCK_MASK = 0x100000

# Each node/path has
# - A SET keyed by path containing a lock
# - A key named 'l' + path, that contains a lock
# - A lock encodes a global clock snapshot and an expiry
# Steps:
# 1. First, remove any expired locks contained by the SET of the most nested path. If there are any live locks in the SET, then abort.
# 2. For each path and subpath, remove any expired locks mapped to by a lock key 'l' + ... . If there are any live locks, then abort.
# 3. If we pass through step 1 and 2 without aborting, then create a single lock string that encodes (1) an incremented global lock clock and (2) an expiry
# 4. For each path and subpath, add this single lock string to the SETs associated with the paths and subpaths.
# 5. Fetch the transaction log since the incoming txn ver.
# 6. Return [lock string, truncated since transaction log]
JournalRedis.LOCK = LOCK = """
local now = os.time()
local path = KEYS[1]
for i, lock in pairs(redis.call('smembers', path)) do
  if lock % #{LOCK_TIMEOUT_MASK} < now then
    redis.call('srem', path, lock)
  else
    return 0
  end
end
for i, path in pairs(KEYS) do
  path = 'l' .. path
  local val = redis.call('get', path)
  if val then
    if val % #{LOCK_TIMEOUT_MASK} < now then
      redis.call('del', path)
    else
      return 0
    end
  end
end
local lock = '0x' ..
  string.format('%x', redis.call('incr', 'lockClock') % #{LOCK_CLOCK_MASK}) ..
  string.format('%x', now + #{LOCK_TIMEOUT})
redis.call('set', 'l' .. path, lock)
for i, path in pairs(KEYS) do
  redis.call('sadd', path, lock)
end
local txns
if ARGV[1] ~= '' then txns = redis.call('zrangebyscore', 'txns', ARGV[1], '+inf') end
return {lock, txns}
"""

JournalRedis.UNLOCK = UNLOCK = """
local val = ARGV[1]
local path = 'l' .. KEYS[1]
if redis.call('get', path) == val then redis.call('del', path) end
for i, path in pairs(KEYS) do
  redis.call('srem', path, val)
end
"""

JournalRedis.LOCKED_COMMIT = LOCKED_COMMIT = """
local val = ARGV[1]
local path = 'l' .. KEYS[1]
local fail = false
if redis.call('get', path) == val then redis.call('del', path) else fail = true end
for i, path in pairs(KEYS) do
  if redis.call('srem', path, val) == 0 then return 0 end
end
if fail then return 0 end
local ver = redis.call('incr', 'ver')
redis.call('zadd', 'txns', ver, ARGV[2])
return ver
"""
