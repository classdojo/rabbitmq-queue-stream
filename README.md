# rabbitmq-queue-stream

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
  queueStream: {
    name: "myQueue",
    prefetchCount: 100
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
    myProcessingStream._transform(function(data, enc, next) {
      console.log("Doing something with", data);
      this.push(data);
      next();
    });

    channel.source
      .pipe(myProcessingStream)
      .pipe(channel.sink);
  });

  /* example graceful shutdown routine */
  var gracefulShutdown = function() {
    //stop fetching messages
    streamifiedQueues.unsubscribeConsumers(function(err) {
      if(err) {
        //handle error
      }
      //Wait some time for queues to flush out before closing consumers.
      streamifiedQueues.closeConsumers(function(err) {
        if(err) {
          //handle error
        }
        streamifiedQueues.disconnect(function(err) {
          if(err) {
            //handle error
          }
          process.exit(0);
        });
      });
    });
  };
});
```

### Emitted Events

#### .source
* parseError - Emitted when a job cannot be json parsed. Passes in malform
```javascript
myQueueStream.source.on("parseError", function(err, message) {
  console.error("Problem JSON parsing message", message);
});
```

#### .sink
* deleted - Emitted everytime a job is deleted from the queue
```javascript
var totalDeleted = 0;
myQueueStream.source.on("deleted", function() {
  console.log("Deleted", totalDeleted++);
});
```
* formatError - Sink received a job that does not have the necessary information to be deleted from the queue.  
  Most likely emitted when objects not originating from .source are written to sink.
```javascript
myQueueStream.sink.on("formatError", function(err, message) {
  console.error("Malformatted message written to .sink. Please check your pipeline configuration", message);
});
```

### TODO

* Add a jsonMode to make automatic parsing of source data optional

