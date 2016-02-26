'use strict';

/* global describe it */
var should = require('should');
var through2 = require('through2');
var async = require('async');

var lib = require('..');
var plugins = lib.plugins;


/**
 * Creates a test suite for a given plugin.
 *
 * This is necessary in order to close over the plugin in scope.
 *
 * @param {Object} plugin - A plugin object containing `Producer` and `Consumer`.
 * @return {Function} - A function with the relevant plugin closed over to allow
 * properly scoped async iteration.
 */
function getSuite(plugin) {
  return function() {
    var Producer = plugins[plugin].Producer;
    var Consumer = plugins[plugin].Consumer;
    describe(plugin, function() {
      it('should read message from the consumer that were written to the producer', function(done) {
        var stream = through2.obj();
        var instantiator = function(Plugin, cb) {
          new Plugin({topic: 'test', stream}, cb);
        };
        async.parallel({producer: instantiator.bind(null, Producer), consumer: instantiator.bind(null, Consumer)}, function(error, plugins) {
          var producer = plugins.producer;
          var consumer = plugins.consumer;
          var cleanup = function(done) {
            async.parallel([producer.destroy, consumer.destroy], done);
          };
          var consumedEvents = [];
          consumer.stream.pipe(through2.obj(function(data, enc, cb) {
            this.push(data);
            consumedEvents.push(data);
            if (consumedEvents.length === 3) {
              setTimeout(function() {
                try {
                  should.exist(consumedEvents);
                  consumedEvents.length.should.equal(3);
                  consumedEvents[0].foo.should.equal('bar');
                  consumedEvents[1].baz.should.equal('bot');
                  consumedEvents[2].captain.should.equal('spock');
                  cleanup(done);
                }
                catch (e) {
                  cleanup(done);
                  throw e;
                }
              // Note: this is set such that the autocommit interval will be met.
              }, 5);
            }
            cb();
          }));
          should.exist(producer.stream);
          producer.stream.write({foo: 'bar'});
          producer.stream.write({baz: 'bot'});
          producer.stream.end({captain: 'spock'});
        });
      });
    });
  };
}

describe('Plugins', function() {
  for (let plugin in plugins) {
    if (plugins.hasOwnProperty(plugin)) {
      getSuite(plugin)();
    }
  }
});
