'use strict'
var through2 = require('through2')

/**
 * An event producer that receives events and dispatches them to a handler class.
 */
class Producer {

  /**
   * @param {object} options - A hash of options for the producer.
   * @param {object} handler - An instantiated handler to delegate event emission to.
   */
  constructor(options) {
    var localOptions = options || {}
    this.validateOptions(localOptions)
    this.inputStream = through2(this.streamHandler.bind(this))
    // TODO: Support a default?
    this.handler = localOptions.handler
  }

  validateOptions(options) {
    if (!options.handler) {
      throw new Error('You must specify a handler when instantiating an event Producer().')
    }
  }

  streamHandler(data, enc, done) {
    this.handler.produce(data, function(error) {
      done()
    })
  }

  getStream() {
    return this.inputStream
  }
}

module.exports = Producer
