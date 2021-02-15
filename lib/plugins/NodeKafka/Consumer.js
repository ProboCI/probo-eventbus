'use strict';

var through2 = require('through2');
var _stream = Symbol('_stream');
var _rawStream = Symbol('_rawStream');
var _ = require('lodash');
const stream = require('stream');

class KafkaConsumer {

  /**
   * @param {Object.<string, any>} config - An object with configuration
   *   options.
   * @param {Object.<string, string>} options.kafkaClientOptions - Options to be
   *   passed to kafka-node's Client() constructor.
   * @param {string} options.version - Version number to be included with
   *   message envelope, coerced to a string. Defaults to empty string.
   */
  constructor(config) {

    this.config = config || {};
    this.validateConfig(config);

    this.version = config.version || '';
    this.topic = config.topic;
    this.group = config.group;

    this.kafkaClientOptions = config.kafkaClientOptions || {};
    this.kafkaConsumerOptions = config.kafkaConsumerOptions || {};

    this.consumer = null;

    this.connectToKafka();
  }

  validateConfig() {
    if (!this.config.topic) {
      throw new Error('Topic must be specified');
    }

    if (!this.config.group) {
      throw new Error('Consumer group must be specified');
    }
  }

  connectToKafka() {

    const kafka = require('kafka-node');
    const config = this.config;

    this.client = new kafka.Client(config.connectionString,
                                   config.clientId,
                                   config.zkOptions,
                                   config.noAckBatchOptions);

    let topics = [{
      topic: this.topic,
    }];

    let consumerOptions = {
      autoCommit: false,
      groupId: this.group,
    };

    this.client = new kafka.Client(config.connectionString,
                                   config.clientId,
                                   config.zkOptions,
                                   config.noAckBatchOptions);

    this.consumer = new kafka.Consumer(this.client, topics, _.merge(consumerOptions, this.kafkaConsumerOptions));

    // Recovers from offset out of range errors by setting offset to latest
    // messages.
    this.consumer.on('offsetOutOfRange', err => {
      console.log('Recovering from offsetOutofRange error', err);

      let topic = err.topic;
      let partition = err.partition;

      let offset = new kafka.Offset(this.client);
      offset.fetch([
          { topic: topic, partition: partition}
      ], (err, data) => {
        this.consumer.setOffset(topic, partition, data[topic][partition]);
      });

    });
  }

  destroy(done) {
    this.consumer.close(false, () => {
      this.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;
