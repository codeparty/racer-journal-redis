{expect} = require 'racer/test/util'
racer = require 'racer'
shouldBehaveLikeJournalAdapter = require 'racer/test/journalAdapter'

plugin = require('../src')

options = journal: type: 'Redis'

describe 'Redis journal adapter', ->
  shouldBehaveLikeJournalAdapter options, [plugin]

  describe 'deletion', ->
    beforeEach (done) ->
      racer.use plugin
      @store = racer.createStore journal: type: 'Redis'
      @store.flush done

    afterEach (done) ->
      @store.flush done

    it 'should delete all redis client keys', (done) ->
      store = @store
      store.set 'color', 'green', 1, ->
        redisClient = store._journal._redisClient
        redisClient.keys '*', (err, value) ->
          # Note that flush calls redisInfo.onStart immediately after
          # flushing, so the key 'starts' should exist
          expect(value).to.only.contain 'txns', 'ver', 'lockClock', 'starts'
          store.flushJournal (err) ->
            expect(err).to.be.null()
            redisClient.keys '*', (err, value) ->
              # Note that flush calls redisInfo.onStart immediately after
              # flushing, so the key 'starts' should exist
              expect(value).to.only.contain 'starts'
              done()
