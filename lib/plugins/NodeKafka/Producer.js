'use strict';

const through2 = require('through2');

const _stream = Symbol('_stream');

class KafkaProducer {

  /**
   * @param {Object.<string, any>} config - An object with configuration
   *   options.
   * @param {Object.<string, string>} options.streamOptions - An object with
   *   stream options to be passed to the transform stream constructor.
   * @param {Object.<string, string>} options.kafkaClientOptions - Options to be
   *   passed to kafka-node's Client() constructor.
   * @param {string} options.version - Version number to be included with
   *   message envelope, coerced to a string. Defaults to empty string.
   */
  constructor(config) {

    this.config = config || {};
    this.version = (config.version || '') + '';
    this.topic = config.topic;
    let streamOptions = config.streamOptions || {highWaterMark: 1000};

    this.kafkaClientOptions = config.kafkaClientOptions || {};
    this.kafkaProducerOptions = config.kafkaProducerOptions || {};

    this.destroy = this.destroy.bind(this);
    this[_stream] = through2.obj(streamOptions, (data, _, cb) => {
      this.streamProcessor(data, cb);
    });

    this.connectToKafka();
  }

  /**
   * Connects to Kafka server and creates the producer.
   */
  connectToKafka() {

    this.validateConfig();

    // Note, kafka-node is a conditional requirement but this file is loaded
    // when the module is included so we require kafka only when needed.
    const kafka = require('kafka-node');
    const config = this.config;

    this.client = new kafka.Client(config.connectionString,
                                   config.clientId,
                                   config.zkOptions,
                                   config.noAckBatchOptions);

    this.producer = new kafka.Producer(this.client, this.kafkaProducerOptions);
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
    // Add our envelope on top of the original data.
    let payload = [{
      topic: this.topic,
      messages: JSON.stringify({
        version: this.version,
        data: data,
      }),
    }];

    this.producer.send(payload, () => {
      cb();
    });

  }

  get stream() {
    return this[_stream];
  }

  destroy(done) {
    this.client.close(done);
  }
}

module.exports = KafkaProducer;
