redis = require 'redis'
{expect} = require 'racer/test/util'
redisInfo = require '../src/redisInfo'

describe 'redisInfo', ->
  # Capture console errors in testing
  __consoleError = console.error

  checkFirstStart = (starts) ->
    expect(starts.length).to.eql 1
    startId = starts[0][0]
    ver = starts[0][1]
    [startTime, startsLength] = startId.split '.'
    expect(new Date - startTime).to.be.within 0, 100
    expect(startsLength).to.eql '0'
    expect(ver).to.eql '0'

  client = null
  subClient = null

  beforeEach (done) ->
    client = redis.createClient()
    subClient = redis.createClient()
    client.flushdb done

  afterEach (done) ->
    client.flushdb ->
      client.end()
      subClient.end()
      done()
  
  it 'getStarts should work with an uninitialized Redis instance', (done) ->
    redisInfo._getStarts client, (err, starts) ->
      expect(err).to.be.null()
      checkFirstStart starts
      done()
  
  # TODO: Uncomment once the racer redis loader is written
  # it 'getStarts should log an error on an uninitialized Redis instance', (done) ->
  #   console.error = (message) ->
  #     expect(message).to.be.a 'string'
  #     console.error = __consoleError
  #     done()
  #   redisInfo._getStarts client, ->
  
  it 'calling getStarts multiple times should work after calling onStart', (done) ->
    redisInfo.onStart client, ->
      client.lrange 'starts', 0, -1, (err, starts) ->
        starts1 = (start.split ',' for start in starts)
        # Delay to make sure start timestamp is different if it gets reset
        setTimeout ->
          redisInfo._getStarts client, (err, starts2) ->
            expect(err).to.be.null()
            expect(starts1).to.eql starts2
            setTimeout ->
              redisInfo._getStarts client, (err, starts3) ->
                expect(err).to.be.null()
                expect(starts1).to.eql starts3
                done()
            , 10
        , 10
  
  it 'onStart should capture the current version when it is called', (done) ->
    client.set 'ver', 7, ->
      redisInfo.onStart client, ->
        client.set 'ver', 13, ->
          redisInfo._getStarts client, (err, starts) ->
            expect(err).to.be.null()
            ver = starts[0][1]
            expect(ver).to.eql '7'
            done()
  
  it 'subscribeToStarts should return a list of starts immediately', (done) ->
    redisInfo.onStart client, ->
      redisInfo.subscribeToStarts subClient, client, (err, starts) ->
        expect(err).to.be.null()
        checkFirstStart starts
        done()
  
  it 'subscribeToStarts should callback on set of starts', (done) ->
    redisInfo.onStart client, ->
      count = 0
      redisInfo.subscribeToStarts subClient, client, (err, starts) ->
        expect(err).to.be.null()
        return checkFirstStart starts unless count++
        expect(starts.length).to.eql 2
        startId = starts[0][0]
        [startTime, startsLength] = startId.split '.'
        expect(startsLength).to.eql '1'
        done()
      redisInfo.onStart client
