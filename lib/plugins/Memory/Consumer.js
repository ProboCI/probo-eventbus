'use strict';

var stream = require('stream');
var Transform = stream.Transform;

var through2 = require('through2');

var MemoryProducer = require('./Producer');

var _rawStream = Symbol('_rawStream');
var _stream = Symbol('_stream');

class MemoryConsumer extends MemoryProducer {

  constructor(options, done) {
    super(options, done);
    var streamOptions = options.streamOptions || {highWaterMark: 1000};
    this[_rawStream] = options.stream || through2.obj(streamOptions);
    this[_stream] = false;
  }

  createCommitStream(options) {
    var commitStream = new Transform({
      objectMode: true,
      write: function (chunk, encoding, done) {
        if (this.passthrough) {
          this.push(chunk);
        }
        done();
      },
    });
    commitStream.passthrough = options.passthrough;
    commitStream.on('pipe', function() {
      this.passthrough = true;
    });
    commitStream.commit = function(cb) {
      cb();
    };
    return commitStream;
  }

  get rawStream() {
    return this[_rawStream];
  }

  get stream() {
    if (!this[_stream]) {
      this[_stream] = through2.obj(function(data, enc, cb) {
        cb(null, data.data);
      });
      this[_rawStream].pipe(this[_stream]);
    }
    return this[_stream];
  }

}

module.exports = MemoryConsumer;
