'use strict'

var kafka = require('kafka-node')
var Producer = kafka.Producer
var client = new kafka.Client()
var producer = new Producer(client)

class Kafka {
  constructor() {
  }
}

payloads = [
  { topic: 'topic1', messages: 'hi ' + Date.now(), partition: 0 }
]
producer.on('ready', function () {
  setInterval(function() {
    producer.send(payloads, function (err, data) {
      console.log('sent data', data)
    })
  }, 500)
})
