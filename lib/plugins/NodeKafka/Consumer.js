'use strict';

var through2 = require('through2');
var _stream = Symbol('_stream');
var _ = require('lodash');

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;

class KafkaConsumer {

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
    this.validateOptions(options, done);
    options = options || {};
    this.topic = options.topic;
    this.group = options.group;
    this[_stream] = through2.obj();
    var self = this;
    var kafkaClientOptions = options.kafkaClientOptions || {};
    this.connectToKafka(kafkaClientOptions, function(error) {
      if (done) done(error, self);
    });
    this.kafkaConsumerOptions = options.kafkaConsumerOptions || {};
    this.destroy = this.destroy.bind(this);
  }

  validateOptions(options, done) {
    if (typeof done !== 'function') {
      done = function(error) {
        throw error;
      };
    }
    if (!options.topic) {
      return done(new Error('Topic must be specified'));
    }
    if (!options.group) {
      return done(new Error('Consumer group must be specified'));
    }
  }

  connectToKafka(options, done) {
    this.client = new kafka.Client();
    var topics = [
      {topic: this.topic},
    ];
    var consumerOptions = {
      autoCommit: true,
      group: this.group,
    };
    this.consumer = new Consumer(this.client, topics, _.merge(consumerOptions, this.kafkaConsumerOptions));
    this.consumer.on('message', this.messageHandler.bind(this));
    done();
  }

  messageHandler(message) {
    try {
      message = JSON.parse(message.value);
      this[_stream].write(message);
    }
    catch (e) {
      // TODO: This should be error event emission (to the stream???) or else some kind of loggging?
      // console.error('JSON PARSE FAILURE: ' + message);
      console.error(e);
    }
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
    var self = this;
    self.consumer.close(true, function() {
      self.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;
