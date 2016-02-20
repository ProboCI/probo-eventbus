'use strict';

var through2 = require('through2')
var _stream = Symbol('_stream');

class MemoryConsumer {
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this[_stream] = options.stream || through2();
    if (done) done();
  }
  get stream() {
    return this[_stream];
  }
}

module.exports = MemoryConsumer;
