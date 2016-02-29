'use strict';

var through2 = require('through2');
var _stream = Symbol('_stream');

var kafka = require('kafka-node');
var Producer = kafka.Producer;

class KafkaProducer {

  /**
   * @param {Object} options - An object with configuration options.
   * @param {Object} options. - An object with configuration options.
   * @param {Mixed} options.kafkaClientOptions - Options to be passed to kafka-node's Client() constructor.
   * @param {Mixed} options.version - Version number to be included with message envelope, coerced to a string. Defaults to empty string.
   * @param {Function} done - A function to call after the producer has been properly constructed.
   */
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this.version = (options.version || '') + '';
    this.topic = options.topic;
    this.validateOptions(options, done);
    this[_stream] = through2.obj(this.streamProcessor.bind(this));
    var self = this;
    var kafkaClientOptions = options.kafkaClientOptions || {};
    this.connectToKafka(kafkaClientOptions, function(error) {
      if (done) done(error, self);
    });
    this.kafkaProducerOptions = options.kafkaProducerOptions || {};
    this.destroy = this.destroy.bind(this);
  }

  validateOptions(options, done) {
    if (typeof done !== 'function') {
      done = function(error) {
        throw error;
      };
    }
    if (!options.topic) {
      done(new Error('Topic must be specified'));
    }
  }

  connectToKafka(options, done) {
    this.client = new kafka.Client(options.connectionString, options.clientId, options.zkOptions, options.noAckBatchOptions);
    this.producer = new Producer(this.client, this.kafkaProducerOptions);
    this.producer.on('ready', function() {
      done();
    });
  }

  streamProcessor(data, enc, done) {
    // add our envelope on top of the original data

    var message = {
      topic: this.topic,
      messages: JSON.stringify({
        version: this.version,
        data: data,
      }),
    };
    this.producer.send([message], function() {
      done();
    });
  }

  get stream() {
    return this[_stream];
  }

  destroy(done) {
    this.client.close(done);
  }
}

module.exports = KafkaProducer;
