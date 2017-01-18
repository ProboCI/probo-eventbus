'use strict';

/* global describe it */
var should = require('should');
var through2 = require('through2');
var async = require('async');

var lib = require('..');
var plugins = lib.plugins;


/**
 * Creates a test suite for a given plugin.
 *
 * This is necessary in order to close over the plugin in scope.
 *
 * @param {String} plugin - A string representing a plugin name.
 * @param {Object} options - An object with options specified for the Producer and Consumer.
 * @return {Function} - A function with the relevant plugin closed over to allow
 * properly scoped async iteration.
 */
function getSuite(plugin, options) {
  return function() {
    var Producer = plugins[plugin].Producer;
    var Consumer = plugins[plugin].Consumer;
    describe(plugin, function() {
      it('should read messages from the consumer that were written to the producer', function(done) {
        var instantiator = function(Plugin, type, cb) {
          new Plugin(options[type], cb);
        };
        async.parallel({producer: instantiator.bind(null, Producer, 'Producer'), consumer: instantiator.bind(null, Consumer, 'Consumer')}, function(error, plugins) {
          var producer = plugins.producer;
          var consumer = plugins.consumer;
          var consumedEvents = [];
          var commitStream = consumer.createCommitStream({autoCommit: false, passthrough: true});
          var cleanup = function(done) {
            async.series([commitStream.commit, producer.destroy, consumer.destroy], done);
          };
          let eventCount = 0;
          consumer.rawStream
            .pipe(through2.obj(function(data, enc, cb) {
              consumedEvents.push(data);
              cb(null, data);
            }))
            .pipe(commitStream)
            .pipe(through2.obj(function(data, enc, cb) {
              eventCount++;
              cb(null);
              if (eventCount === 23) {
                commitStream.commit(function() {
                  try {
                    should.exist(consumedEvents[0].data.foo, 'The first message should have a foo property');
                    consumedEvents[0].data.foo.should.equal('bar');
                    consumedEvents[1].data.baz.should.equal('bot');
                    consumedEvents[2].data.line.should.equal(1);
                    consumedEvents[21].data.line.should.equal(20);
                    consumedEvents[22].data.captain.should.equal('spock');
                    cleanup(done);
                  }
                  catch (e) {
                    cleanup(function() {
                      done(e);
                    });
                  }
                });
              }
              else if (eventCount > 23) {
                cleanup(done.bind(this, new Error('More than 23 events received')));
              }
            }))
          should.exist(producer.stream);
          producer.stream.write({foo: 'bar'});
          producer.stream.write({baz: 'bot'});
          for (let line = 1; line < 21; line++) {
            producer.stream.write({line});
          }
          producer.stream.end({captain: 'spock'});
        });
      });
    });
  };
}

describe('Plugins', function() {
  // getSuite('Kafka')();
  var memoryOptions = {
    stream: through2.obj(),
    topic: 'test',
    version: 1,
  };
  getSuite('Memory', {Producer: memoryOptions, Consumer: memoryOptions})();
  var kafkaOptions = {
    Producer: {
      topic: 'test1',
      version: 1,
    },
    Consumer: {
      topic: 'test1',
      group: 'test',
      kafkaConsumerOptions: {
        autoCommit: false,
        autoCommitIntervalMs: 5,
      },
    },
  };
  getSuite('Kafka', kafkaOptions)();
});
