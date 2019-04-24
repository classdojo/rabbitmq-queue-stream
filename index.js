var _                 = require("lodash");
var amqp              = require("amqp");
var async             = require("async");
var debug             = require("debug");
var Readable          = require("stream").Readable;
var Writable          = require("stream").Writable;
var Transform         = require("stream").Transform;
var EventEmitter      = require('events').EventEmitter;

var streamsDebug      = debug("rabbitmq-queue-stream:streams");


exports.init = function(numStreams, options, cb) {
  if(!cb) {
    cb = options;
    options = {};
  }
  var streams = new AMQPStreams(numStreams, options);
  streams.initialize(cb);
};


var RequeueMessage = function(message) {
  if(!message._meta) {
    return console.error();
  }
  message._meta.requeue = true;
  return message;
};

var RejectMessage = function(message) {
  if(!message._meta) {
    return console.error();
  }
  message._meta.reject = true;

  return message;
};

exports.RequeueMessage = RequeueMessage;
exports.RejectMessage = RejectMessage;

/*

  @param numStreams
  @params options

    options.url - amqp url

    options.connection.* - Anything accepted by amqp.createConnection's first arg (See: https://github.com/postwait/node-amqp#connection-options-and-url)
    options.nodeAmqp.* - Anything accepted by amqp.createConnection's second arg (See: https://github.com/postwait/node-amqp#connection-options-and-url)

    options.queue.connection.* - Anything accepted by connection.queue() (See: https://github.com/postwait/node-amqp#connectionqueuename-options-opencallback)
      DEFAULT: { passive: true }

    options.queue.subscribe.* - Anything accepted by queue.subscribe() (See: https://github.com/postwait/node-amqp#queuesubscribeoptions-listener)
      DEFAULT: { ack: true, prefetchCount: 1 }
*/
function AMQPStreams(numStreams, options) {
  EventEmitter.call(this);

  this.__numStreams = numStreams || 1;
  this.__options = options;
  this.channels = [];
}
AMQPStreams.prototype = Object.create(EventEmitter.prototype);

AMQPStreams.prototype.initialize = function(cb) {
  streamsDebug("Initializing " + this.__numStreams + " streams");
  var me = this;
  this._createConnection(this.__options.connection, this.__options.nodeAmqp, function(err, connection) {
    if(err) {
      return cb(err);
    }
    me._amqpConnection = connection;
    me._connected = false;

    connection.on('ready', function() {
      me._connected = true;
    });

    connection.on('close', function() {
      me._connected = false;
    });

    // forward connection events to `this`
    ['ready', 'error', 'close'].forEach(function(eventName) {
      connection.on(eventName, function() {
        // ignore error events while disconnecting, since those errors will be forwarded to
        // disconnect's callback. See #disconnect()
        if (eventName === 'error' && me._disconnecting) {
          return;
        }

        var args = Array.prototype.slice.call(arguments);
        args.unshift(eventName);
        me.emit.apply(me, args);
      });
    });

    //create individual stream channels to queue
    var createWorker = function(n, cb) {
      AMQPStream.create(connection, me.__options.queue, cb);
    };

    async.timesSeries(me.__numStreams, createWorker, function(err, channels) {
      if(err) {
        return cb(err);
      }
      me.channels = channels;
      cb(null, me);
    });
  });
};


AMQPStreams.prototype._createConnection = function(connectionOpts, implOptions, cb) {
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

  var connection = amqp.createConnection(_.merge(defaultOpts, connectionOpts), implOptions || {});

  function onError(err) {
    streamsDebug("Error creating connection " + err.message);
    connection.removeListener("ready", onReady);
    return cb(err);
  }

  function onReady() {
    streamsDebug("Successfully created connection");
    connection.removeListener("error", onError);
    return cb(null, connection);
  }

  /* handle successful or error on initial connection */
  connection.once("error", onError);
  connection.once("ready", onReady);
};



/*
 * NOTE: A graceful disconnection routine from rabbitMQ should be
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
 * Helper method that strings together the above disconnection
 * routine.
*/
AMQPStreams.prototype.gracefulDisconnect = function(cb) {
  async.series([
    this.unsubscribeConsumers.bind(this),
    this.closeConsumers.bind(this),
    this.disconnect.bind(this)
  ], cb);
};

/*
 * Stops fetching messages from the queue. Channels are kept open.
 * Use AMQPStreams#closeConsumers to close the channels to queue.
*/
AMQPStreams.prototype.unsubscribeConsumers = function(cb) {
  // noop if we're disconnected
  if (!this._connected) {
    streamsDebug("Skipping unsubscribeConsumers");
    return cb();
  }

  //close every worker stream
  async.eachSeries(this.channels, function(stream, next) {
    stream.unsubscribe(next);
  }, cb);
};

/*
 * One AMQP Connection is multiplexed across multiple
 * channels. This method closes only the channel
 * corresponding to this queue stream. See AMQP#disconnect
 * to close the actual AMQP connection. You should safely
 * unsubsribe all queues before disconnecting from the
 * AMQP server.
 *
*/
AMQPStreams.prototype.closeConsumers = function(cb) {
  // noop if we're disconnected
  if (!this._connected) {
    streamsDebug("Skipping closeConsumers");
    return cb();
  }

  async.eachSeries(this.channels, function(stream, next) {
    stream.close(next);
  }, cb);
};

AMQPStreams.prototype.disconnect = function(cb) {
  streamsDebug("Closing AMQP connection");
  // noop if we're disconnected
  if (!this._connected) {
    streamsDebug("Skipping disconnect");
    return cb();
  }

  this._disconnecting = true;
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

    if(_.includes(err.message, "ECONNRESET")) {
      streamsDebug("Ignoring ECONNRESET error");
      return;
    }
    cb(err);
  };

  this._amqpConnection.once("error", ignoreEconnresetError);
  this._amqpConnection.once("close", function() {
    this._disconnecting = false;
    me._amqpConnection.removeListener("error", ignoreEconnresetError);
    cb();
  });
};


/*
 * Resubscribe queue consumers.
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
function AMQPStream(connection, options, workerNum) {
  this.id = workerNum;
  this.__connection = connection;
  this.__options = options || {};
  this.__outstandingAcks = [];
  this.__pendingQueue = [];
  this.__debug = debug("rabbitmq-queue-stream:worker:" + (workerNum || "-"));
}

AMQPStream.create = function(connection, options, cb) {
  this.__totalWorkers = this.__totalWorkers || 0;
  this.__totalWorkers++;
  var stream = new this(connection, options, this.__totalWorkers);
  stream.initialize(cb);
};

AMQPStream.prototype.initialize = function(cb) {
  var me = this;
  this.__debug("Initializing");
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
        me.__debug("Error subscribe to queue " + this.__options.name + ". " + err.message);
        return cb(err);
      }
      me._streamifyQueue(cb);
    });
  });
};


AMQPStream.prototype._connectToQueue = function(queueName, cb) {
  var me = this;
  function onError(err) {
    me.__debug("Error connecting to queue " + queueName + ": " + err.message);
    return cb(err);
  }
  this.__connection.once("error", onError);
  this.__connection.queue(queueName, _.merge({passive: true}, this.__options.connection), function(queue) {
    me.__debug("Connected to queue " + queueName);
    me.__connection.removeListener("error", onError);
    return cb(null, queue);
  });
};

AMQPStream.prototype._subscribeToQueue = function(cb) {
  var me = this;
  var queue = this.__queue;
  /* TODO: Figure out how to error handle subscription. Maybe a 'once' error handler. */
  queue.subscribe(
    _.merge({ack: true, prefetchCount: 1}, this.__options.subscribe),
    this._handleIncomingMessage.bind(this)
  ).addCallback(function(ok) {
    me.__debug("Subscribed with consumer tag: " + ok.consumerTag);
    me.__consumerTag = ok.consumerTag;
    me.subscribed = true;
    cb(null, me);
  });
};


AMQPStream.prototype._handleIncomingMessage = function(message, headers, deliveryInfo, ack) {
  var isJSON = deliveryInfo.contentType === "application/json";
  var serializableMessage = {
    payload: isJSON ? message : message.data,
    headers: headers,
    deliveryInfo: deliveryInfo,
    _meta: {
      /*
       * ack is not serializable, so we need to push it
       * onto the outstandingAck array attach
       * an ackIndex number to the message
      */
      ackIndex: this._insertAckIntoArray(ack)
    }
  };

  if(isJSON && deliveryInfo.parseError) {
    this.sink.write(RejectMessage(serializableMessage));
    if(this.source.listeners('parseError').length) {
      return this.source.emit("parseError", deliveryInfo.parseError, deliveryInfo.rawData);
    } else {
      console.error("Automatically rejecting malformed message. " +
                  "Add listener to 'parseError' for custom behavior");
      return;
    }
  }
  this.__debug("Received message. Inserted ack into index " + serializableMessage._meta.ackIndex);
  this.__pendingQueue.push(serializableMessage);
};

AMQPStream.prototype._streamifyQueue = function(cb) {
  var queueStream, prepareMessage;
  var me = this;
  var queue = this.__queue;
  var sink;


  /* Create the .source ReadableStream */
  queueStream = new Readable({objectMode: true});
  queueStream._read = function() {
    me.__debug("_read .source");
    me._waitForMessage(function(message) {
      this.push(message);
    }.bind(this));
  };

  prepareMessage = new Transform({objectMode: true});
  prepareMessage._transform = function(message, enc, next) {
    this.push(_.pick(message, "payload", "_meta"));
    next();
  };
  this.source = queueStream.pipe(prepareMessage);

  /* Create the .sink WritableStream */
  sink = new Writable({objectMode: true});
  sink._write = function(message, enc, next) {
    me.__debug("_write .sink");
    if(!message._meta || !_.isNumber(message._meta.ackIndex)) {
      me.__debug("Could not find ackIndex in message " + message);
      return this.emit("formatError", new Error("No ack index for message"), message);
    }
    var ackIndex = message._meta.ackIndex;
    if(!me.__outstandingAcks[ackIndex]) {
      //something went wrong and we can't ack message
      me.__debug("Could not find ack function for " + message);
      return this.emit("ackError", new Error("Cannot find ack for message."), message);
    }
    var evt;
    if(message._meta.requeue) {
      me.__outstandingAcks[ackIndex].reject(true);
      evt = "requeued";
    } else if(message._meta.reject) {
      me.__outstandingAcks[ackIndex].reject(false);
      evt = "rejected";
    } else {
      me.__outstandingAcks[ackIndex].acknowledge(false);
      evt = "acknowledged";
    }
    me.__outstandingAcks[ackIndex] = null;
    this.emit(evt, message);
    next();
  };
  this.sink = sink;

  cb(null, this);
};

AMQPStream.prototype._waitForMessage = function(cb) {
  var i;
  var me = this;
  if(_.isEmpty(this.__pendingQueue)) {
    this.__debug("Waiting for message");
    i = setInterval(function() {
      if(!_.isEmpty(me.__pendingQueue)) {
        clearInterval(i);
        me.__debug("Received messages. Continuing...");
        return cb(me.__pendingQueue.shift());
      }
    }, 5);
  } else {
    this.__debug("Dequeueing pending message");
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
  this.__debug("Unsubscribing with consumerTag " + this.__consumerTag);
  if(this.subscribed) {
    this.__queue.unsubscribe(this.__consumerTag).addCallback(function() {
      me.subscribed = false;
      cb();
    });
  } else {
    this.__debug("Worker already unsubscribed");
    cb();
  }
};

AMQPStream.prototype.close = function(cb) {
  var me = this;
  this.__debug("Unsubscribing with consumerTag " + this.__consumerTag);
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


/*
 * Test helper that responds with with a mock `source`
 * and `sink` properties. Only returns one channel.
*/
exports.createWithTestMessages = function(testMessages) {
  var stubSource = new Readable({objectMode: true});
  stubSource._read = function() {
    var itemWrapper;
    var nextItem = testMessages.shift();
    if(!nextItem) {
      return this.push(null);
    }
    itemWrapper = {
      headers: {},
      deliveryInfo: {},
      payload: nextItem,
      _meta: {}
    };
    this.push(itemWrapper);
  };

  var stubSink = new Writable({objectMode: true});
  stubSink._write = function(item, enc, next) {
    var evt;
    if(item._meta.requeue) {
      evt = "requeued";
    } else if(item._meta.reject) {
      evt = "rejected";
    } else {
      evt = "acknowledged";
    }
    this.emit(evt, item);
    next();
  };
  return {
    channels: [{
      source: stubSource,
      sink: stubSink
    }]
  };
};

/* export for testing */
exports.AMQPStreams = AMQPStreams;
exports.AMQPStream  = AMQPStream;
