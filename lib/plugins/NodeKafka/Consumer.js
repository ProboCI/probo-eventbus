'use strict';

class KafkaConsumer {

  constructor(config) {
    this.config = config || {};
    this.validateConfig();

    this.consumer = null;
    this.kafka = null;
    this._messageListener = null;
    this._errorListener = null;
    this._latestOffsets = new Map();
  }

  validateConfig() {
    if (!this.config.consumerTopic) {
      throw new Error('The consumer topic (consumerTopic) must be specified');
    }
    if (!this.config.groupId) {
      throw new Error('Consumer group (groupId) must be specified');
    }
  }

  async start() {
    const {Kafka} = require('kafkajs');

    this.kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
    });

    this.consumer = this.kafka.consumer({groupId: this.config.groupId});

    this.consumer.on(this.consumer.events.CRASH, e => {
      const err = (e.payload && e.payload.error) || new Error('Kafka consumer crashed');
      if (this._errorListener) this._errorListener(err);
    });

    // kafkajs requires the consumer to have joined the group before seek()
    // will succeed. Skip past pre-existing messages on first join only.
    let initialSeekDone = false;
    this.consumer.on(this.consumer.events.GROUP_JOIN, async () => {
      if (initialSeekDone) return;
      initialSeekDone = true;
      try {
        const admin = this.kafka.admin();
        await admin.connect();
        const offsets = await admin.fetchTopicOffsets(this.config.consumerTopic);
        await admin.disconnect();
        for (const {partition, high} of offsets) {
          this.consumer.seek({topic: this.config.consumerTopic, partition, offset: high});
        }
      }
      catch (err) {
        if (this._errorListener) this._errorListener(err);
      }
    });

    await this.consumer.connect();
    await this.consumer.subscribe({topic: this.config.consumerTopic, fromBeginning: false});
  }

  onMessage(listener) {
    this._messageListener = listener;

    this.consumer.run({
      eachMessage: async ({topic, partition, message}) => {
        this._latestOffsets.set(partition, message.offset);
        if (!this._messageListener) return;
        try {
          const envelope = JSON.parse(message.value.toString());
          this._messageListener(envelope.data !== undefined ? envelope.data : envelope);
        }
        catch (err) {
          if (this._errorListener) this._errorListener(err);
        }
      },
    }).catch(err => {
      if (this._errorListener) this._errorListener(err);
    });
  }

  onError(listener) {
    this._errorListener = listener;
  }

  async commit() {
    if (!this.consumer || this._latestOffsets.size === 0) return;

    const offsets = Array.from(this._latestOffsets.entries()).map(([partition, offset]) => ({
      topic: this.config.consumerTopic,
      partition,
      offset: (BigInt(offset) + 1n).toString(),
    }));

    return this.consumer.commitOffsets(offsets);
  }

  async destroy(cb) {
    try {
      if (this.consumer) await this.consumer.disconnect();
      if (cb) cb();
    }
    catch (err) {
      if (cb) cb(err);
    }
  }
}

module.exports = KafkaConsumer;
