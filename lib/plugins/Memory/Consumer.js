'use strict';

var through2 = require('through2')
var _stream = Symbol('_stream');

var MemoryProducer = require('./Producer');

class MemoryConsumer extends MemoryProducer {
}

module.exports = MemoryConsumer;
