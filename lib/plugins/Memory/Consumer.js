'use strict';

// This files

const through2 = require('through2');

const _rawStream = Symbol('_rawStream');

class MemoryConsumer {

  constructor(options) {
    const streamOptions = options.streamOptions || {highWaterMark: 1000};
    this[_rawStream] = options.stream || through2.obj(streamOptions);
  }

  start() {
    return Promise.resolve();
  }

  onMessage(listener) {
    this[_rawStream].on('data', data => {
      listener(data.data);
    });
  }

  commit() {}

  destroy(cb) {
    cb();
  }

}

module.exports = MemoryConsumer;
