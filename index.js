var amqp              = require("amqp");
var async             = require("async");
var _                 = require("lodash");
var streamsDebug      = require("debug")("amqp-streams");
var streamInitDebug   = require("debug")("amqp-stream-init");
var streamDebug       = require("debug")("amqp-stream"); //stream runtime debug
var systemDebug       = require("debug")("system");

var Transform = require("stream").Transform;
var Readable  = require("stream").Readable;
var Writable  = require("stream").Writable;




exports.init = function(numStreams, options, cb) {
  if(!cb) {
    cb = options;
    options = {};
  }
  streams = new AMQPStreams(numStreams, options);
  streams.initialize(cb);
};


exports.RequeueMessage = function(message) {
  if(!message._meta) {
    return console.error();
  }
  message._meta.requeue = true;
  return message;
};

exports.DeleteMessage = function(message) {
  if(!message._meta) {
    return console.error();
  }
  message._meta.delete = true;
  return message; 
};

/*

  @param numStreams
  @params options

    options.connection.* - Anything accepted by amqp.createConnection

    options.queueStream.* - Anything accepted by connection.queue()

    options.url - amqp url
*/
function AMQPStreams(numStreams, options) {
  this.__numStreams = numStreams || 1;
  this.__options = options;
  this.channels = [];
}


AMQPStreams.prototype.initialize = function(cb) {
  streamsDebug("Initializing " + this.__numStreams + " streams");
  var me = this;
  this._createConnection(this.__options.connection, function(err, connection) {
    if(err) {
      return cb(err);
    }
    me._amqpConnection = connection;

    //create individual stream channels to queue
    var createWorker = function(n, cb) {
      AMQPStream.create(connection, me.__options.queueStream, cb);
    };

    async.timesSeries(me.__numStreams, createWorker, function(err, insightStreams) {
      if(err) {
        return cb(err);
      }
      me.channels = insightStreams;
      cb(null, me);
    });
  });
};


AMQPStreams.prototype._createConnection = function(connectionOpts, cb) {
  streamsDebug("Creating amqp connection.");
  connectionOpts = connectionOpts || {};
  var defaultOpts = {
    heartbeat: 10,
    clientProperties: {
      capabilities: {
        consumer_cancel_notify: true
      }
    }
  };

  var connection = amqp.createConnection(_.merge(defaultOpts, connectionOpts));

  /* handle successful or error on initial connection */
  connection.once("error", function(err) {
    streamsDebug("Error creating connection " + err.message);
    connection.removeAllListeners("ready");
    return cb(err);
  });

  connection.once("ready", function() {
    streamsDebug("Successfully created connection");
    connection.removeAllListeners("error");
    return cb(null, connection);
  });

};


/*
 * NOTE: A proper disconnection routine from rabbitMQ should be
 * done in the following order:
 *
 *  AMQPStreams#unsubscribeConsumers - Tells queue to stop
 *    delivering messages to the queue consumer.
 *  
 *  AMQPStreams#closeConsumers- Closes the channel between
 *    the consumer and queue.
 *
 *  AMQPStreams#disconnect- Closes the actual TCP connection
 *    to the AMQP source.
*/

/*
 * Stops fetching messages from the queue. Channels are kept open.
 * Use AMQPStreams#closeConsumers to close the channels to queue.
*/
AMQPStreams.prototype.unsubscribeConsumers = function(cb) {
  //close every worker stream
  async.eachSeries(this.channels, function(stream, next) {
    stream.unsubscribe(next);
  }, cb);
};

/*
 * One AMQP Connection is multiplexed across multiple
 * channels. This method closes only the channel
 * corresponding to this queue stream. See AMQP#disconnect
 * to close the actual AMQP connection.  You should safely
 * unsubsribe all queues before disconnecting from the
 * AMQP server.
 *
*/
AMQPStreams.prototype.closeConsumers = function(cb) {
  async.eachSeries(this.channels, function(stream, next) {
    stream.close(next);
  }, cb);
};

AMQPStreams.prototype.disconnect = function(cb) {
  streamsDebug("Closing AMQP connection");
  var me = this;
  this._amqpConnection.disconnect();
  var ignoreEconnresetError = function(err) {

    /* 
     *  Driver has a bug on some versions of RabbitMQ
     *  and node combinations where socket.close()
     *  causes an ECONNRESET. Catch and ignore
     *  that error for now. More info:
     *
     *  https://github.com/postwait/node-amqp/issues/300
    */

    if(_.contains(err.message, "ECONNRESET")) {
      streamDebug("Ignoring ECONNRESET error");
      return;
    }
    cb(err);
  };

  this._amqpConnection.once("error", ignoreEconnresetError);
  this._amqpConnection.once("close", function() {
    me._amqpConnection.removeListener("error", ignoreEconnresetError);
    cb();
  });
};


/*
 * Resubscribe queue consumers. Should be called after
 */
AMQPStreams.prototype.resubscribeConsumers = function(cb) {
  async.eachSeries(this.channels, function(stream, next) {
    if(!stream.subscribed) {
      stream._subscribeToQueue(next);
    } else {
      next();
    }
  }, cb);
};


/* 
  @param connection = An object returned by amqp.createConnection
  @param options
    options.queueName
*/
function AMQPStream(connection, options) {
  this.__connection = connection;
  this.__options = options || {};
  this.__outstandingAcks = [];
  this.__pendingQueue = [];
}

AMQPStream.create = function(connection, options, cb) {
  this._totalWorkers = this._totalWorkers || 0;
  this._totalWorkers++;
  streamInitDebug("Creating stream " + this._totalWorkers);
  var insightStream = new this(connection, options);
  insightStream.initialize(cb);
};

AMQPStream.prototype.initialize = function(cb) {
  var me = this;
  streamInitDebug("Initializing");
  if(!this.__options.name) {
    throw new Error("You must provide a `name` to queueStream options object");
  }
  this._connectToQueue(this.__options.name, function(err, queue) {
    if(err) {
      return cb(err);
    }
    me.__queue = queue;

    //attach a general queueErrorHandler here
    queue.on("error", me.__options.onError || function() {});

    //Last step is to streamify this queue by attaching stream .source and .sink properties
    me._subscribeToQueue(function(err) {
      if(err) {
        streamInitDebug("Error subscribe to queue " + this.__options.name + ". " + err.message);
        return cb(err);
      }
      me._streamifyQueue(cb);
    });
  });
};


AMQPStream.prototype._connectToQueue = function(queueName, cb) {
  var me = this;
  this.__connection.once("error", function(err) {
    streamInitDebug("Error connecting to queue " + queueName + ": " + err.message);
    return cb(err);
  });
  this.__connection.queue(queueName, {passive: true}, function(queue) {
    streamInitDebug("Connected to queue " + queueName);
    me.__connection.removeAllListeners("error");
    return cb(null, queue);
  });
};

AMQPStream.prototype._subscribeToQueue = function(cb) {
  var me = this;
  var queue = this.__queue;
  var subscribeOptions = {ack: true, prefetchCount: 1};
  if(this.__options.prefetchCount) {
    subscribeOptions.prefetchCount = this.__options.prefetchCount;
  }
  /* TODO: Figure out how to error handle subscription. Maybe a 'once' error handler. */
  queue.subscribe(subscribeOptions, function(message, headers, deliveryInfo, ack) {
    var serializableMessage = {
      body: message.data,
      headers: headers,
      deliveryInfo: deliveryInfo
    };

    /*
     * ack is not serializable, so we need to push it
     * onto the outstandingAck array right now and attach
     * an ackIndex number to the message
    */
    serializableMessage.ackIndex = me._insertAckIntoArray(ack);
    streamDebug("Received message. Inserted ack into index " + serializableMessage.ackIndex);
    me.__pendingQueue.push(serializableMessage);
  }).addCallback(function(ok) {
    streamDebug("Subscribed with consumer tag: " + ok.consumerTag);
    me.__consumerTag = ok.consumerTag;
    me.subscribed = true;
    cb(null, me);
  });
};

AMQPStream.prototype._streamifyQueue = function(cb) {
  var queueStream, prepareMessage;
  var me = this;
  var queue = this.__queue;

  streamInitDebug("Creating queue source");
  
  queueStream = new Readable({objectMode: true});
  queueStream._read = function() {
    systemDebug("_read queueStream");
    me._waitForMessage(function(message) {
      this.push(message);
    }.bind(this));
  };

  /*
    TODO: add json mode

    Transform gets a messages:
      {
        body: String. In our case JSON parsable.
        headers: ,
        deliveryInfo: ,
        ackIndex: Number
      }

    This only pushes down the parsed body. It attaches the worker
    id into meta
  */
  streamInitDebug("Creating readable queue stream");
  prepareMessage = new Transform({objectMode: true});
  prepareMessage._transform = function(message, enc, next) {
    var parsedBody;
    systemDebug("_transform prepareMessage");
    try {
      parsedBody = JSON.parse(message.body.toString());
    } catch(e) {
      return this.emit("parseError", e, message);
    }

    parsedBody._meta = parsedBody._meta || {};
    parsedBody._meta.ackIndex = message.ackIndex;
    this.push(parsedBody);
    next();
  };
  this.source = queueStream.pipe(prepareMessage);

  //add sink to instance
  var sink;
  streamInitDebug("Creating queue sink");
  sink = new Writable({objectMode: true});
  sink._write = function(message, enc, next) {
    systemDebug("_write sink");
    if(!message._meta || !_.isNumber(message._meta.ackIndex)) {
      streamDebug("Could not find ackIndex in message " + message);
      return this.emit("formatError", new Error("No ack index for message"), message);
    }
    var ackIndex = message._meta.ackIndex;
    if(!me.__outstandingAcks[ackIndex]) {
      //something went wrong and we can't ack message
      streamDebug("Could not find ack function for " + message);
      return this.emit("ackError", new Error("Cannot find ack for message."), message);
    }
    /* TODO: How do we handle errors from acking? */

    var eventName;
    if(message._meta.requeue) {
      me.__outstandingAcks[ackIndex].reject(true);
      eventName = "requeued";
    } else if(message._meta.delete) {
      me.__outstandingAcks[ackIndex].reject(false);
      eventName = "deleted";
    } else {
      me.__outstandingAcks[ackIndex].acknowledge(false);
      eventName = "acknowledged";
    }

    me.__outstandingAcks[ackIndex] = null;
    this.emit(eventName, message);
    next();
  };
  this.sink = sink;
  cb(null, this);
};

AMQPStream.prototype._waitForMessage = function(cb) {
  var i;
  var me = this;
  if(_.isEmpty(this.__pendingQueue)) {
    streamDebug("Waiting for message");
    i = setInterval(function() {
      if(!_.isEmpty(me.__pendingQueue)) {
        clearInterval(i);
        streamDebug("Received messages. Continuing...");
        return cb(me.__pendingQueue.shift());
      }
    }, 5);
  } else {
    streamDebug("Dequeueing pending message");
    cb(this.__pendingQueue.shift());
  }
};

AMQPStream.prototype._insertAckIntoArray = function(ack) {
  for(var i = 0; i < this.__outstandingAcks.length; i++) {
    if(!this.__outstandingAcks[i]) {
      this.__outstandingAcks[i] = ack;
      return i;
    }
  }
  return this.__outstandingAcks.push(ack) - 1;
};


/*
 * Unsubscribes from the queue and also closes
 * the channel.
*/
AMQPStream.prototype.unsubscribe = function(cb) {
  var me = this;
  streamDebug("Unsubscribing with consumerTag " + this.__consumerTag);
  if(this.subscribed) {
    this.__queue.unsubscribe(this.__consumerTag).addCallback(function() {
      me.subscribed = false;
      cb();
    });
  } else {
    streamDebug("Worker already unsubscribed");
    cb();
  }
};

AMQPStream.prototype.close = function(cb) {
  var me = this;
  streamDebug("Unsubscribing with consumerTag " + this.__consumerTag);
  this.__queue.close(this.__consumer);
  var closeHandler = function() {
    me.__queue.removeListener("error", errorHandler);
    cb();
  };
  var errorHandler = function(err) {
    me.__queue.removeListener("close", closeHandler);
    return cb(err);
  };
  this.__queue.once("close", closeHandler);
  this.__queue.once("error", errorHandler);
};

/* export for testing */
exports.AMQPStreams = AMQPStreams;
exports.AMQPStream  = AMQPStream;
