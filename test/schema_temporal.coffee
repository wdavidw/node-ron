
should = require 'should'

try config = require '../conf/test' catch e
ron = require '../lib'

client = Users = null

before (next) ->
  client = ron config
  next()
  
afterEach (next) ->
  client.redis.keys '*', (err, keys) ->
    should.not.exists err
    keys.should.eql []
    next()

after (next) ->
  client.quit next

describe 'type', ->

  it 'should deal with create', (next) ->
    Records = client.get
      name: 'records'
      temporal: true
      properties: 
        record_id: identifier: true
    date = new Date Date.UTC 2008, 8, 10, 16, 5, 10
    Records.clear (err) ->
      Records.create {}, (err, record) ->
        should.not.exist err
        # should v0.6.3 is broken with "instanceof Date"
        # https://github.com/visionmedia/should.js/issues/65
        (record.cdate instanceof Date).should.be.true
        (record.mdate instanceof Date).should.be.true
        # record.cdate.should.be.an.instanceof Date
        # record.mdate.should.be.an.instanceof Date
        Records.clear next

  it 'should deal with update', (next) ->
    Records = client.get
      name: 'records'
      temporal: true
      properties: 
        record_id: identifier: true
    date = new Date Date.UTC 2008, 8, 10, 16, 5, 10
    Records.clear (err) ->
      Records.create {}, (err, record) ->
        cdate = record.cdate
        Records.update record, (err, record) ->
          should.not.exist err
          # should v0.6.3 is broken with "instanceof Date"
          # https://github.com/visionmedia/should.js/issues/65
          (record.cdate instanceof Date).should.be.true
          (record.mdate instanceof Date).should.be.true
          # record.cdate.should.be.an.instanceof Date
          # record.mdate.should.be.an.instanceof Date
          # (record.cdate is cdate).should.be.true
          record.cdate.getTime().should.eql cdate.getTime()
          Records.clear next
