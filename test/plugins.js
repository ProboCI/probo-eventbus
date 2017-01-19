'use strict';

/* global describe it */
var should = require('should');
var through2 = require('through2');
var async = require('async');

var kafka = require('kafka-node');

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
function getSuite(plugin, options, beforeCallback, afterCallback) {
  return function() {
    if (beforeCallback) {
      before(beforeCallback);
    }
    if (afterCallback) {
      after(afterCallback);
    }
    var Producer = plugins[plugin].Producer;
    var Consumer = plugins[plugin].Consumer;
    describe(plugin, function() {
      it('should read messages from the consumer that were written to the producer', function(done) {
        var instantiator = function(Plugin, type, cb) {
          new Plugin(options[type], cb);
        };
        async.parallel({producer: instantiator.bind(null, Producer, 'Producer'), consumer: instantiator.bind(null, Consumer, 'Consumer')}, function(error, plugins) {
          let producer = plugins.producer;
          let consumer = plugins.consumer;
          let rawStreamEvents = [];
          let commitStream = consumer.createCommitStream({autoCommit: false, passthrough: true});
          let cleanup = function(done) {
            async.series([commitStream.commit, producer.destroy, consumer.destroy], done);
          };
          let eventCount = 0;
          let regularStreamEvents = [];
          consumer.stream.pipe(through2.obj(function(data, enc, cb) {
            regularStreamEvents.push(data);
            cb();
          }));
          consumer.rawStream
            .pipe(through2.obj(function(data, enc, cb) {
              rawStreamEvents.push(data);
              cb(null, data);
            }))
            .pipe(commitStream)
            .pipe(through2.obj(function(data, enc, cb) {
              eventCount++;
              cb(null);
              if (eventCount === 23) {
                commitStream.commit(function() {
                  try {
                    regularStreamEvents.length.should.equal(23);
                    regularStreamEvents[0].foo.should.equal('bar');
                    regularStreamEvents[1].baz.should.equal('bot');
                    regularStreamEvents[2].line.should.equal(1);
                    regularStreamEvents[21].line.should.equal(20);
                    should.exist(rawStreamEvents[0].data.foo, 'The first message should have a foo property');
                    rawStreamEvents.length.should.equal(23);
                    rawStreamEvents[0].data.foo.should.equal('bar');
                    parseInt(rawStreamEvents[0].version).should.equal(1);
                    rawStreamEvents[1].data.baz.should.equal('bot');
                    rawStreamEvents[2].data.line.should.equal(1);
                    rawStreamEvents[21].data.line.should.equal(20);
                    rawStreamEvents[22].data.captain.should.equal('spock');
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
  const topic = '_eventbus_kafka_' + Date.now();
  var kafkaOptions = {
    Producer: {
      topic,
      version: 1,
    },
    Consumer: {
      topic,
      group: 'test',
      kafkaConsumerOptions: {
        autoCommit: false,
        autoCommitIntervalMs: 5,
      },
    },
  };
  let createTopic = function(done) {
    let client = new kafka.Client();
    let producer = new kafka.Producer(client);
    producer.on('ready', function() {
      producer.createTopics([topic], done);
    });
  };
  getSuite('Kafka', kafkaOptions, createTopic)();
});
