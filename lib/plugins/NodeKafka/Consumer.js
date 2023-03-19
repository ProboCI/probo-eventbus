'use strict';

const _ = require('lodash');

class KafkaConsumer {

  constructor(config) {
    this.consumer = null;
    this.config = config || {};
    this.validateConfig(config);
    this.connectToKafka();
  }

  validateConfig() {
    if (!this.config.consumerTopic) {
      throw new Error('The consumer topic (consumerTopic) must be specified');
    }
    if (!this.config.groupId) {
      throw new Error('Consumer group (groupId) must be specified');
    }
  }

  connectToKafka() {
    // Note, kafkajs is a conditional requirement but this file is loaded
    // when the module is included so we require kafka only when needed.
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
    });

    const consumer = kafka.consumer({ groupId: this.config.groupId });
    consumer.connect();
    consumer.subscribe({ topic: this.consumerTopic, fromBeginning: false });
    this.consumer = consumer;
  }

  destroy(done) {
    this.consumer.close(false, () => {
      this.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;

