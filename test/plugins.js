'use strict';

/* global describe it */
const async = require('async');
const kafka = require('kafka-node');
const should = require('should');
const through2 = require('through2');

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
  return () => {
    if (beforeCallback) {
      before(beforeCallback);
    }
    if (afterCallback) {
      after(afterCallback);
    }

    let Producer = plugins[plugin].Producer;
    let Consumer = plugins[plugin].Consumer;

    describe(plugin, () => {
      it('should read messages from the consumer that were written to the producer', done => {
        let instantiator = (Plugin, type, cb) => {
          new Plugin(options[type], cb);
        };

        async.parallel({producer: instantiator.bind(null, Producer, 'Producer'), consumer: instantiator.bind(null, Consumer, 'Consumer')}, (error, plugins) => {
          let producer = plugins.producer;
          let consumer = plugins.consumer;
          let rawStreamEvents = [];
          let commitStream = consumer.createCommitStream({autoCommit: false, passthrough: true});
          let cleanup = done => {
            async.series([commitStream.commit, producer.destroy, consumer.destroy], done);
          };
          let eventCount = 0;
          let regularStreamEvents = [];
          consumer.stream.pipe(through2.obj((data, enc, cb) => {
            regularStreamEvents.push(data);
            cb();
          }));

          consumer.rawStream
            .pipe(through2.obj((data, enc, cb) => {
              rawStreamEvents.push(data);
              cb(null, data);
            }))
            .pipe(commitStream)
            .pipe(through2.obj((data, enc, cb) => {
              eventCount++;
              cb(null);
              if (eventCount === 23) {
                commitStream.commit(() => {
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
                    cleanup(() => {
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

function getSuite2(plugin, options, beforeCallback, afterCallback) {
  return () => {
    if (beforeCallback) {
      before(beforeCallback);
    }
    if (afterCallback) {
      after(afterCallback);
    }

    let Producer = plugins[plugin].Producer;
    let Consumer = plugins[plugin].Consumer;

    describe(plugin, () => {
      it('should read messages from the consumer that were written to the producer', done => {
        let instantiator = (Plugin, type, cb) => {
          new Plugin(options[type], cb);
        };

        async.parallel({producer: instantiator.bind(null, Producer, 'Producer'), consumer: instantiator.bind(null, Consumer, 'Consumer')}, (error, plugins) => {
          let producer = plugins.producer;
          let consumer = plugins.consumer;
          let rawStreamEvents = [];
          let commitStream = consumer.createCommitStream({autoCommit: false, passthrough: true});
          let cleanup = done => {
            async.series([commitStream.commit, producer.destroy, consumer.destroy], done);
          };
          let eventCount = 0;
          let regularStreamEvents = [];
          consumer.stream.pipe(through2.obj((data, enc, cb) => {
            console.log(data);
            regularStreamEvents.push(data);
            cb();
          }));
          console.log('here');

          //console.log(consumer.rawStream);

          consumer.rawStream
            .pipe(through2.obj((data, enc, cb) => {
              console.log('here2');
              return done();
              rawStreamEvents.push(data);
              cb(null, data);
            }))
            .pipe(commitStream)
            .pipe(through2.obj((data, enc, cb) => {
              console.log('here3');
              eventCount++;
              cb(null);
              if (eventCount === 23) {
                commitStream.commit(() => {
                  try {
                    console.log('here4');
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
                    cleanup(() => {
                      console.log('here5');
                      done(e);
                    });
                  }
                });
              }
              else if (eventCount > 23) {
                console.log('here6');
                cleanup(done.bind(this, new Error('More than 23 events received')));
              }
            }));

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

describe('Plugins', () => {
  // getSuite('Kafka')();
  var memoryOptions = {
    stream: through2.obj(),
    topic: 'test',
    version: 1,
  };
  getSuite('Memory', {Producer: memoryOptions, Consumer: memoryOptions})();

  const topic = '_eventbus_kafka_' + Date.now();
  let kafkaOptions = {
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

  let createTopic = done => {
    let client = new kafka.Client();
    let producer = new kafka.Producer(client);
    producer.on('ready', () => {
      producer.createTopics([topic], done);
    });
  };

  getSuite2('Kafka', kafkaOptions, createTopic)();
});
