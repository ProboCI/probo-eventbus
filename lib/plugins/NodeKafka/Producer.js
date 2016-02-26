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
   * @param {Function} done - A function to call after the producer has been properly constructed.
   */
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this.topic = options.topic;
    this.validateOptions(options, done);
    this[_stream] = through2.obj(this.streamProcessor.bind(this));
    var self = this;
    var kafkaClientOptions = options.kafkaClientOptions || {};
    this.connectToKafka(kafkaClientOptions, function(error) {
      if (done) done(error, self);
    });
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
    this.client = new kafka.Client(options.kafkaClient);
    this.producer = new Producer(this.client);
    this.producer.on('ready', function() {
      done();
    });
  }

  streamProcessor(data, enc, done) {
    var message = {
      topic: this.topic,
      messages: JSON.stringify(data),
    };
    this.producer.send([message], done);
  }

  get stream() {
    return this[_stream];
  }

  destroy(done) {
    this.client.close(done);
  }
}

module.exports = KafkaProducer;
