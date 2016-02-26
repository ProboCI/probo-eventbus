# Probo EventBus

This library provides an abstraction for object stream transport
allowing you to have a pluggable backend for transporting event
streams from one services or part of your application to another.

This library is a framework under active development for use in
connecting services in an SOA using a CQRS style message
passing over replaceable message transpors (currently an in-memory
wrapper around [through2](http://npmjs.com/package/through2), in
and a wrapper around [node-kafka](http://npmjs.com/package/node-kafka)
are provided.