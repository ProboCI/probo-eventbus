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

  async connectToKafka() {
    // Note, kafkajs is a conditional requirement but this file is loaded
    // when the module is included so we require kafka only when needed.
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
    });

    const consumer = kafka.consumer({ groupId: this.config.groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: this.config.consumerTopic, fromBeginning: false });

    // Seek to latest offsets so we don't replay old messages on restart
    const admin = kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchTopicOffsets(this.config.consumerTopic);
    await admin.disconnect();

    for (const { partition, high } of offsets) {
      consumer.seek({ topic: this.config.consumerTopic, partition, offset: high });
    }

    this.consumer = consumer;
  }

  destroy(done) {
    this.consumer.close(false, () => {
      this.client.close(done);
    });
  }
}

module.exports = KafkaConsumer;

