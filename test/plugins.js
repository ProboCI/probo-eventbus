'use strict'

/* global describe it */
var should = require('should')
var through2 = require('through2')
var async = require('async');

var lib = require('..')
var plugins = lib.plugins;


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

/**
 * Creates a test suite for a given plugin.
 *
 * This is necessary in order to close over the plugin in scope.
 */
function getSuite(plugin) {
  return function() {
    var Producer = plugins[plugin].Producer;
    var Consumer = plugins[plugin].Consumer;
    describe(plugin, function() {
      it('should read message from the consumer that were written to the producer', function(done) {
        var stream = through2.obj();
        var instantiator = function(Plugin, cb) {
          new Plugin({}, cb);
        };
        async.parallel({producer: instantiator.bind(null, Producer), consumer: instantiator.bind(null, Consumer)}, function() {
          var producer = new Producer({stream});
          var consumer = new Consumer({stream});
          var cleanup = function(done) {
            async.parallel([producer.destroy, consumer.destroy], done);
          };
          consumer.stream.pipe(historyStream(function(error, events) {
            if (error) cleanup(done);
            should.exist(events);
            events.length.should.equal(3);
            events[0].should.equal('data');
            events[1].foo.should.equal('bar');
            events[2].should.equal('more data');
            cleanup(done);
          }));
          should.exist(producer.stream);
          producer.stream.write('data');
          producer.stream.write({foo: 'bar'});
          producer.stream.end('more data');
        });
      })
    });
  };
}

describe('Plugins', function() {
  for (let plugin in plugins) {
    getSuite(plugin)();
  }
})
