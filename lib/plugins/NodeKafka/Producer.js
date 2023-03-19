'use strict';

const through2 = require('through2');

const _stream = Symbol('_stream');

class KafkaProducer {

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
    this.validateConfig();

    this.clientId = config.clientId;
    this.topic = config.topic;
    this.brokers = config.brokers || ['localhost:9092'];
    this.destroy = this.destroy.bind(this);

    this[_stream] = through2.obj((data, _, cb) => {
      this.streamProcessor(data, cb);
    });

    this.connectToKafka();
  }

  /**
   * Connects to Kafka server and creates the producer.
   */
  connectToKafka() {

    // Note, kafkajs is a conditional requirement but this file is loaded
    // when the module is included so we require kafka only when needed.
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({
      clientId: this.clientId,
      brokers: [this.brokers],
    });

    this.producer = kafka.producer();
    this.producer.connect();
  }

  /**
   * Validates configuration object.
   *
   * @return {true} - If configuration object is valid.
   * @throw - If configuration is not valid.
   */
  validateConfig() {
    if (!this.config.topic) {
      throw Error('Topic must be specified');
    }
    return true;
  }

  /**
   * Process the data stream.
   *
   * @param {Object.<string, any>} - The event data to be emitted.
   * @param {() => void} - The callback function after data was sent.
   */
  streamProcessor(data, cb) {
    let payload = {
      topic: data.topic,
      messages: [
        { value: JSON.stringify({ data: data }) },
      ],
    };
    this.producer.send(payload);
    cb();
  }

  get stream() {
    return this[_stream];
  }

  destroy(done) {
    this.producer.disconnect();
  }
}

module.exports = KafkaProducer;
