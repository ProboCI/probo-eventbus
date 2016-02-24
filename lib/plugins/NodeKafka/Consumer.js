'use strict';

var through2 = require('through2')
var _stream = Symbol('_stream');

var KafkaProducer = require('./Producer');

class KafkaConsumer extends KafkaProducer {
}

module.exports = KafkaConsumer;
