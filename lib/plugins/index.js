'use strict';
module.exports = {}
module.exports.Memory = require('./Memory');
// Requiring ./Kafka segfaults. That's weird.
module.exports.Kafka = require('./NodeKafka');
