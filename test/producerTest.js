'use strict'
var should = require('should')
var events = require('events')
var EventEmitter = events.EventEmitter

var ProboEvents = require('..')
var Producer = ProboEvents.Producer

class ProducerPlugin extends EventEmitter {
  constructor() {
    super()
  }
  produce(data, cb) {
    this.emit('event', data)
    if (cb) {
      cb()
    }
  }
}

describe('Producer', function() {
  it('Should write an event to the configured handler plugin', function(done) {
    var producerPlugin = new ProducerPlugin()
    var producer = new Producer({ handler: producerPlugin })
    var produceStream = producer.getStream()
    producerPlugin.on('event', done.bind(this, null))
    produceStream.write('data')
  })
})
