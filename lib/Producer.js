'use strict'
var through2 = require('through2')

/**
 * An event producer that receives events and dispatches them to a handler class.
 *
 * @param {object} options - A hash of options for the producer.
 * @param {object} handler - An instantiated handler to delegate event emission to.
 */
var Producer = function(options) {
  var localOptions = options || {}
  this.validateOptions(localOptions)
  this.inputStream = through2(this.streamHandler.bind(this))
  this.handler = localOptions.handler
}

Producer.prototype.validateOptions = function(options) {
  if (!options.handler) {
    throw new Error('You must specify a handler when instantiating an event Producer().')
  }
}

Producer.prototype.streamHandler = function(data, enc, done) {
  this.handler.produce(data, function(error) {
    done()
  })
}

Producer.prototype.getStream = function() {
  return this.inputStream
}

module.exports = Producer
