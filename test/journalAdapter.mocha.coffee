{expect} = require 'racer/test/util'

require('racer/test/journalAdapter') {type: 'Redis'}, require('../src'), (run) ->

  run 'Redis journal flushing', (getStore) ->
    it 'should delete all redis client keys', (done) ->
      store = getStore()
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
