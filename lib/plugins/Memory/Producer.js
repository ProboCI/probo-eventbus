'use strict';

var through2 = require('through2');
var _stream = Symbol('_stream');

class MemoryProducer {

  /**
   * @param {Object} options - An options object.
   * @param {Object} options.streams - An object contining streams for each named topic..
   * @param {Function} done - A function to call after the producer has been properly constructed.
   */
  constructor(options, done) {
    if (typeof options === 'function') {
      done = options;
      options = {};
    }
    options = options || {};
    var streamOptions = options.streamOptions || {highWaterMark: 1000};
    this[_stream] = options.stream || through2.obj(streamOptions);
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
