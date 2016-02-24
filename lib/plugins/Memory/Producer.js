'use strict';

var through2 = require('through2')
var _stream = Symbol('_stream');

class MemoryProducer {
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    this[_stream] = options.stream || through2();
    if (done) done(null, this);
    this.destroy = this.destroy.bind(this);
  }
  get stream() {
    return this[_stream];
  }
  destroy(done) {
    done();
  }
}

module.exports = MemoryProducer;
