'use strict';

var through2 = require('through2');
var _stream = Symbol('_stream');
var _decoratorStream = Symbol('_decoratorStream');

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
    this.topic = options.topic;
    this.version = options.version || 1;
    this.streamOptions = options.streamOptions || {highWaterMark: 1000};
    this[_stream] = options.stream || through2.obj(this.streamOptions);
    this[_decoratorStream] = false;
    if (done) done(null, this);
    this.destroy = this.destroy.bind(this);
  }

  streamProcessor(data, enc, done) {
    // Add our envelope on top of the original data.
    var message = {
      version: this.version,
      data: data,
    };
    done(null, message);
  }

  get stream() {
    if (!this[_decoratorStream]) {
      this[_decoratorStream] = through2.obj(this.streamOptions, this.streamProcessor.bind(this));
      this[_decoratorStream].pipe(this[_stream]);
    }
    return this[_decoratorStream];
  }

  destroy(done) {
    done();
  }
}

module.exports = MemoryProducer;
