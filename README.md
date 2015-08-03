# rabbitmq-queue-stream
[![Build Status](https://travis-ci.org/classdojo/rabbitmq-queue-stream.svg?branch=master)](https://travis-ci.org/classdojo/rabbitmq-queue-stream)
[![codecov.io](https://codecov.io/github/classdojo/rabbitmq-queue-stream/coverage.svg?branch=master)](https://codecov.io/github/classdojo/rabbitmq-queue-stream?branch=master)
[![NPM version](https://badge.fury.io/js/rabbitmq-queue-stream.png)](http://badge.fury.io/js/rabbitmq-queue-stream)


### Tests
```bash
$ make test
```

### Usage
```bash
$ npm i rabbitmq-queue-stream
```

```javascript
var RabbitMQStream = require("rabbitmq-queue-stream");
var stream         = require("stream");

var options = {
  connection: {
    url: "amqp://user:password@rabbitmq.com"
  },
  nodeAmqp: {
    reconnect: false // defaults to true, see https://github.com/postwait/node-amqp#connection-options-and-url (search for reconnect)
  }
  queue: {
    name: "myQueue",
    subscribe: {
      /* Any option accepted by https://github.com/postwait/node-amqp#queuesubscribeoptions-listener */
    },
    connection: {
      /* Any option accepted by https://github.com/postwait/node-amqp#connectionqueuename-options-opencallback */
    }
  }
};

/*
 * Initialize two consumer channels to our queue.
*/
RabbitMQStream.init(2, options, function(err, streamifiedQueues) {
  if(err) {
    return console.error(err);
  }

  /*
   * Each consumer channel comes with a .source and .sink property.
   * 
   * .source is a Readable stream that gives us a stream of objects
   * from the specified queue
   *
   * Every job written to .sink is deleted from the queue. Only object
   * originating from .source should be written to .sink
   *
  */

  streamifiedQueues.channels.forEach(function(channel) {

    var myProcessingStream = new stream.Transform({objectMode: true});
    myProcessingStream._transform(function(obj, enc, next) {
      /*
      * Messages received from the source will have their data namespaced
      * in the `obj.payload` property. `payload` will contain a parsed 
      * JSON object if clients specified contentType: application/json
      * when enqueuing the message. Messages enqueued with contentType:
      * application/json but are malformed will be automatically rejected.
      * Add a listener to event `parseError`, which will be emitted by
      * channel.source, to handle errors yourself.
      */

      this.push(obj);
      /*
       * Messages are successfully acked and removed from the queue by default.
       * RabbitMQStream provides methods to requeue and delete messages too.
       *
       * Requeue:
       *     this.push(RabbitMQStream.RequeueMessage(obj));
       *
       * Reject:
       *     this.push(RabbitMQStream.RejectMessage(obj));
      */
      next();
    });

    channel.source
      .pipe(myProcessingStream)
      .pipe(channel.sink);
  });

  process.on("SIGTERM", function() {
    streamifiedQueues.gracefulDisconnect(function(err) {
      // process.exit
    });
  });

});
```
There also a helper method that helps with integration test

```javascript
var RabbitMQStream = require("rabbitmq-queue-stream");
var Transform = require("stream").Transform;
var myTransformStream = new Transform({objectMode: true});
myTransformStream._transform = function(item, enc, next) {
  console.log("Transforming item:", item);
  this.push(item);
  next();
};
var streamifiedQueues = RabbitMQStream.createWithTestMessages([
  "testMessage1",
  {testMessage: "2"},
  {testMessage: "3"}
]);
/*
 * streamifiedQueues.channels will contain one channel with a
 * streamable .source and .sink.
 */
 var channel = streamifiedQueues.channels.shift();
 channel.source
   .pipe(myTransformStream)
   .pipe(channel.sink);
  
  //channel .sink emits 'requeued', 'rejected', and 'acknowledged' events
  channel.sink.on("acknowledged", console.log.bind(null, "Acknowledged message!"));
```

### Emitted Events

#### AMQPStreams

* ready - AMQP client connected or reconnected
* error - Emitted if connection to broker dies
```javascript
RabbitMQStream.init(2, options, function(err, streamifiedQueues) {
  streamifiedQueues.on('error', function(err) {
    console.error('socket disconnected!');
  });
});
```

#### .source
* parseError - Emitted when a message specifies contentType: application/json but is malformed JSON.
```javascript
myQueueStream.source.on("parseError", function(err, message) {
  console.error("Problem JSON parsing message", message);
});
```

#### .sink
* acknowledged  - Emitted everytime a message is acknowledged
* rejected      - Emitted when a message is rejected
* requeued      - Emitted when a message is requeued
```javascript
var totalAcked = 0;
myQueueStream.source.on("acknowledged", function(message) {
  console.log("Acknowledged:", message);
  totalAcked++;
});
```
* formatError - Sink received a job that does not have the necessary information to be deleted from the queue. Most likely emitted when objects not originating from .source are written to sink.
```javascript
myQueueStream.sink.on("formatError", function(err, message) {
  console.error("Malformatted message written to .sink. Please check your pipeline configuration", message);
});
```

### TODO

* Add a jsonMode to make automatic parsing of source data optional

