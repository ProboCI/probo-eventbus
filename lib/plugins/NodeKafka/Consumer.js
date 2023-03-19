'use strict';

const _ = require('lodash');

class KafkaConsumer {

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

    const kafka = new Kafka({
      clientId: this.config.kafka.clientId,
      brokers: this.config.kafka.brokers,
    });

    const consumer = kafka.consumer({ groupId: this.config.kafka.groupId });

    consumer.connect();
    consumer.subscribe({ topic: this.topic, fromBeginning: false });

    this.consumer = consumer;

  }

  destroy(done) {
    this.consumer.close(false, () => {
      this.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;

