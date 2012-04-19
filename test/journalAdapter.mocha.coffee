{expect} = require 'racer/test/util'
racer = require 'racer'
shouldBehaveLikeJournalAdapter = require 'racer/test/journalAdapter'
{augmentStoreOpts} = require 'racer/test/journalAdapter/util'

plugin = require '../src'

storeOpts =
  mode:
    #type: 'lww' || 'stm' # is provided by the options we merge into
    journal:
      type: 'Redis'
      host: 'localhost'
      db: 'test'

describe 'Redis journal adapter', ->
  shouldBehaveLikeJournalAdapter storeOpts, [plugin]

  describe 'deletion', ->
    beforeEach (done) ->
      opts = augmentStoreOpts storeOpts, 'stm'
      @store = racer.createStore opts
      @store.flush done

    afterEach (done) ->
      @store.flush done

    it 'should delete all redis client keys', (done) ->
      store = @store
      store.set 'color', 'green', 1, ->
        redisClient = store._mode._journal._redisClient
        redisClient.keys '*', (err, value) ->
          # Note that flush calls redisInfo.onStart immediately after
          # flushing, so the key 'starts' should exist
          expect(value).to.only.contain 'txns', 'ver', 'lockClock', 'starts'
          store.flushMode (err) ->
            expect(err).to.be.null()
            redisClient.keys '*', (err, value) ->
              # Note that flush calls redisInfo.onStart immediately after
              # flushing, so the key 'starts' should exist
              expect(value).to.only.contain 'starts'
              done()
