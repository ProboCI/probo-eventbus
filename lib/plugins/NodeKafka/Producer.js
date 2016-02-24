'use strict';

var through2 = require('through2')
var _stream = Symbol('_stream');

var kafka = require('kafka-node');
var Producer = kafka.Producer;

class KafkaProducer {

  /**
   * @param {Object} options - An object with configuration options.
   * @param {Mixed} options.kafkaClient - Options to be passed to kafka-node's Client() constructor.
   */
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this[_stream] = options.stream || through2.obj(this.streamProcessor.bind(this));
    var self = this;
    this.connectToKafka(options, function(error) {
      if (done) done(error, self);
    }.bind(this));
    this.destroy = this.destroy.bind(this);
  }

  connectToKafka(options, done) {
    this.client = new kafka.Client(options.kafkaClient);
    this.producer = new Producer(this.client);
    this.producer.on('ready', done);
  }

  streamProcessor(data, enc, done) {
    var message = {
      topic: 'test',
      messages: JSON.stringify(data),
    };
    console.log(message);
    console.log(this);
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
