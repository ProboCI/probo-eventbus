'use strict'
var events = require('events')
var through2 = require('through2')
var EventEmitter = events.EventEmitter

class Consumer extends EventEmitter {

  /**
   * @param {object} options - A hash of options for the consumer.
   * @param {object} handler - An instantiated handler to delegate event emission to.
   */
  constructor(options) {
    super()
    var localOptions = options || {}
    this.validateOptions(localOptions)
    this.outputStream = through2.obj()
    // TODO: Support a default?
    this.handler = localOptions.handler
    this.handler.on('event', this.consume)
    this.consume = this.consume.bind(this)
  }

  validateOptions(options) {
    if (!options.handler) {
      throw new Error('You must specify a handler when instantiating an event Producer().')
    }
  }

  consume(event) {
    console.log(this)
    console.log('even handled', event)
    this.emit('event2', event)
  }
}
module.exports = Consumer
