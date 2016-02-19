'use strict'

/* global describe it */
var should = require('should')
var through2 = require('through2')

var ProboEvents = require('..')
var Producer = ProboEvents.Producer

var _stream = Symbol('_stream');
class ProducerPlugin {
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this[_stream] = options.stream || through2();
    if (done) done();
  }
  get stream() {
    return this[_stream];
  }
}

class ConsumerPlugin {
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this[_stream] = options.stream || through2();
    if (done) done();
  }
  get stream() {
    return this[_stream];
  }
}

function historyStream(done) {
  var history = [];
  var appender = function(data, enc, cb) {
    history.push(data);
    cb(null, data);
  };
  var reporter = function() {
    if (done) done(null, history);
  };
  return through2.obj(appender, reporter);
}

describe('Producer', function() {
  it('should write an event to the configured handler plugin', function(done) {
    var stream = through2.obj();
    var producer = new ProducerPlugin({stream});
    var consumer = new ConsumerPlugin({stream});
    consumer.stream.pipe(historyStream(function(error, events) {
      should.exist(events);
      events.length.should.equal(3);
      events[0].should.equal('data');
      events[1].foo.should.equal('bar');
      events[2].should.equal('more data');
      done(error);
    }));
    should.exist(producer.stream);
    producer.stream.write('data');
    producer.stream.write({foo: 'bar'});
    producer.stream.end('more data');
  })
})

describe('Consumer', function() {
  it('should receive events from the configured handler plugin', function(done) {
    var consumerPlugin = new ConsumerPlugin()
    return done()
  })
})
