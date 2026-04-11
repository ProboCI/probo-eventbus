'use strict';

const _ = require('lodash');

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

    this.client = new kafka.Client(
      config.connectionString,
      config.clientId,
      config.zkOptions,
      config.noAckBatchOptions
    );

    let consumerOptions = {
      autoCommit: false,
      groupId: this.group,
      fromOffset: true,
    };

    // Fetch the latest offset so we only process new messages on start.
    let offset = new kafka.Offset(this.client);
    offset.fetch([
      { topic: this.topic, partition: 0, time: -1 },
    ], (err, data) => {
      if (err) {
        console.error('Error fetching latest offset:', err);
        return;
      }

      let latestOffset = data[this.topic][0][0];

      let topics = [{
        topic: this.topic,
        offset: latestOffset,
      }];

      this.consumer = new kafka.Consumer(this.client, topics, _.merge(consumerOptions, this.kafkaConsumerOptions));

      // Recovers from offset out of range errors by setting offset to latest
      // messages.
      this.consumer.on('offsetOutOfRange', err => {
        console.log('Recovering from offsetOutofRange error', err);

        let topic = err.topic;
        let partition = err.partition;

        let offsetHelper = new kafka.Offset(this.client);
        offsetHelper.fetch([
          { topic: topic, partition: partition },
        ], (err, data) => {
          if (err) {
            console.error('Error recovering from offsetOutOfRange:', err);
            return;
          }
          this.consumer.setOffset(topic, partition, data[topic][partition]);
        });
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
