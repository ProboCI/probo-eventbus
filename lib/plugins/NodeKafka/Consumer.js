'use strict';

var through2 = require('through2');
var _internalStream = Symbol('_internalStream');
var _stream = Symbol('_stream');
var _rawStream = Symbol('_rawStream');
var _ = require('lodash');

class KafkaConsumer {

  /**
   * @param {Object} options - An object with configuration options.
   * @param {Object} options.streamOptions - An object with stream options to be passed to the transform stream constructor.
   * @param {Mixed} options.kafkaClientOptions - Options to be passed to kafka-node's Client() constructor.
   * @param {Mixed} options.version - If supplied, only read message with this version number in the message envelope, coerced to a string.
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
    this.version = typeof options.version != 'undefined' ? options.version + '' : void 0;
    var streamOptions = options.streamOptions || {highWaterMark: 1000};
    this[_internalStream] = null;
    this[_stream] = null;
    this[_rawStream] = null;
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
    let self = this;

    var kafka = require('kafka-node');
    self.client = new kafka.Client(options.connectionString, options.clientId, options.zkOptions, options.noAckBatchOptions, options.tlsOptions);
    var topics = [
      {topic: self.topic},
    ];
    var consumerOptions = {
      autoCommit: false,
      groupId: self.group,
    };
    self.consumer = new kafka.Consumer(self.client, topics, _.merge(consumerOptions, self.kafkaConsumerOptions));
    var processStream = through2.obj(function(message, enc, cb) {
      try {
        var output = JSON.parse(message.value);
        output.eventbusMetadata = message;
        delete output.eventbusMetadata.value;
        this.push(output);
        cb(null);
      }
      catch (error) {
        cb(error);
      }
    });
    this[_rawStream] = self.consumer
      .pipe(processStream);
    done();
  }

  get stream() {
    if (!this[_stream]) {
      this[_stream] = through2.obj(function(data, enc, cb) {
        cb(null, data.data);
      })
      this.rawStream.pipe(this[_stream]);
    }
    return this[_stream];
  }

  get rawStream() {
    return this[_rawStream];
  }

  createCommitStream(options) {
    var commitStream = this.consumer.createCommitStream(options);
    var extractMetadata = through2.obj(function(data, enc, cb) {
      cb(null, data.eventbusMetadata);
    });
    extractMetadata.commit = function(cb) {
      commitStream.commit(cb);
    };
    extractMetadata.pipe(commitStream)
    return extractMetadata;
  }

  destroy(done) {
    var self = this;
    self.consumer.close(false, function() {
      self.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;
