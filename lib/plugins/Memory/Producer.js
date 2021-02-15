'use strict';

const through2 = require('through2');
const _stream = Symbol('_stream');

class MemoryProducer {

  /**
   * @param {Object} options - An options object.
   * @param {Object} options.streams - An object contining streams for each named topic..
   * @param {Function} done - A function to call after the producer has been properly constructed.
   */
  constructor(options) {
    options = options || {};

    this[_stream] = through2.obj(function(data, _, cb) {
      // Add our envelope on top of the original data.
      const message = {
        version: options.version || 1,
        data: data,
      };

      this.push(message);

      cb();
    });
  }

  commit() {}

  get stream() {
    return this[_stream];
  }

  destroy(done) {
    done();
  }
}

module.exports = MemoryProducer;
