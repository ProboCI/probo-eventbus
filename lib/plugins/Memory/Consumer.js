'use strict';

const eventBus = require('js-event-bus');

const through2 = require('through2');

const _rawStream = Symbol('_rawStream');

class MemoryConsumer {

  constructor(options) {
    // const streamOptions = options.streamOptions || {highWaterMark: 1000};
    // this[_rawStream] = options.stream || through2.obj(streamOptions);

    this.consumer = null;
    this.topic = options.topic || 'build';

    this.connectToEventBus();
  }

  connectToEventBus() {
    this.consumer = eventBus;
  }

  start() {
    return Promise.resolve();
  }

  onMessage(listener) {
    eventBus.on(this.topic, data => {
      listener(data);
    });
  }

  commit() {}

  destroy(cb) {
    cb();
  }

}

module.exports = MemoryConsumer;
