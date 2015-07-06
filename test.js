var EventEmitter    = require("events").EventEmitter;
var rabbitmq        = require("./");
var rewire          = require("rewire");
var expect          = require("expect.js");
var stream          = require("stream");
var sinon           = require("sinon");
var amqp            = require("amqp");
var _               = require("lodash");


describe("rabbitmq-queue-stream", function() {
  describe("AMQPStreams", function() {

    var streamCreateStub;

    beforeEach(function () {
      streamCreateStub = sinon.stub(rabbitmq.AMQPStream, "create");
    });

    afterEach(function () {
      streamCreateStub.restore();
    });

    describe("#initialize", function() {
      var createConnectionStub;

      beforeEach(function () {
        createConnectionStub =
          sinon.stub(rabbitmq.AMQPStreams.prototype, "_createConnection");
      });

      afterEach(function () {
        createConnectionStub.restore();
      });

      it("creates a connection to rabbitmq", function () {
        var connection = {};
        var streams = new rabbitmq.AMQPStreams(1, {
          connection: connection
        });
        streams.initialize();
        expect(createConnectionStub.callCount).to.be(1);
        expect(createConnectionStub.args[0][0]).to.be(connection);
      });


      it("attempts to create the right number of workers", function (done) {
        var connection = new EventEmitter();
        var queue      = {};
        streamCreateStub.yields(null);
        createConnectionStub.yields(null, connection);

        var streams = new rabbitmq.AMQPStreams(6, {
          queue: queue
        });


        streams.initialize(function (err, result) {
          expect(result).to.be(streams);
          expect(streamCreateStub.callCount).to.be(6);
          streamCreateStub.args.forEach(function (argSet) {
            expect(argSet[0]).to.be(connection);
            expect(argSet[1]).to.be(queue);
          });
          done(err);
        });
      });


      it("sets channels if successful", function (done) {
        var connection = new EventEmitter();
        var queue  = {};
        var worker = {};

        createConnectionStub.yields(null, connection);
        streamCreateStub.yields(null, worker);

        var streams = new rabbitmq.AMQPStreams(6, {
          queue: queue
        });

        streams.initialize(function (err, result) {
          expect(result).to.be(streams);
          expect(streams.channels).to.have.length(6);
          streams.channels.forEach( function(channel) {
            expect(channel).to.be(worker);
          });
          done(err);
        });

      });

    });

    describe("#_createConnection", function() {
      var amqpCreateConnectionStub;
      var streams;
      var emitter;

      beforeEach(function() {
        streams = new rabbitmq.AMQPStreams(6, {});
        emitter = new EventEmitter();
        amqpCreateConnectionStub = sinon.stub(amqp, "createConnection");
        amqpCreateConnectionStub.returns(emitter);
      });

      afterEach(function () {
        amqpCreateConnectionStub.restore();
      });

      it("creates a connection with proper defaultOptions", function () {
        streams._createConnection();
        expect(amqpCreateConnectionStub.args[0][0]).to.eql({
          heartbeat: 10,
          clientProperties: {
            capabilities: {
              consumer_cancel_notify: true
            }
          }
        });
      });

      it("creates a connection with overwritten defaults", function () {
        streams._createConnection({
          heartbeat: 5,
          clientProperties: {
            someProp: "someVal"
          }
        });
        expect(amqpCreateConnectionStub.args[0][0]).to.eql({
          heartbeat: 5,
          clientProperties: {
            someProp: "someVal",
            capabilities: {
              consumer_cancel_notify: true
            }
          }
        });
      });

      it("passes back an error if the connection errors out", function () {
        var stub = sinon.stub();
        streams._createConnection({}, stub);
        var err = new Error("I hate you.");
        emitter.emit("error", err);
        expect(stub.callCount).to.be(1);
        expect(stub.args[0][0]).to.be(err);
      });

      it("passes back a connection object if the connection succeeds", function () {
        var stub = sinon.stub();
        streams._createConnection({}, stub);
        emitter.emit("ready");
        expect(stub.args[0][0]).to.be(null);
        expect(stub.args[0][1]).to.be(emitter);
      });

    });


    describe("AMQP queue control methods", function() {
      var amqp;
      beforeEach(function() {
        var streams = [];
        amqp = new rabbitmq.AMQPStreams(6, {});
        _.times(4, function() {
          streams.push({
            unsubscribe: sinon.stub().yields(null),
            close: sinon.stub().yields(null)
          });
        });
        amqp.channels = streams;
      });

      describe("#unsubscribeConsumers", function() {
        it("calls #unsubscribe for every stream in AMQPStreams.channels", function(done) {
          amqp.unsubscribeConsumers(function(err) {
            expect(err).to.not.be.ok();
            amqp.channels.forEach(function(channel) {
              expect(channel.unsubscribe.callCount).to.be(1);
            });
            done();
          });
        });

      });

      describe("#closeConsumers", function() {
        it("calls #close on every stream in AMQPStreams.channels", function(done) {
          amqp.closeConsumers(function(err) {
            expect(err).to.not.be.ok();
            amqp.channels.forEach(function(channel) {
              expect(channel.close.callCount).to.be(1);
            });
            done();
          });
        });
      });

      describe("#disconnect", function() {
        var mockAmqp;
        beforeEach(function() {
          mockAmqp = new EventEmitter();
          mockAmqp.disconnect = sinon.spy();
          amqp._amqpConnection = mockAmqp;
        });

        it("calls #disconnect on the amqp connection", function(done) {
          amqp.disconnect(function(err) {
            expect(err).to.not.be.ok();
            expect(amqp._amqpConnection.disconnect.callCount).to.be(1);
            done();
          });
          //simulate successful close
          amqp._amqpConnection.emit("close");
        });

        it("passes back an error to the callback when something goes wrong", function(done) {
          amqp.disconnect(function(err) {
            expect(err).to.be.an(Error);
            done();
          });
          amqp._amqpConnection.emit("error", new Error("Some disconnection error"));
        });

        it("ignores TCP ECONNRESET errors", function(done) {
          amqp.disconnect(function(err) {
            //will fail if first error event gets through
            expect(err).to.not.be.ok();
            done();
          });
          amqp._amqpConnection.emit("error", new Error("ECONNRESET"));
          amqp._amqpConnection.emit("close");
        });
      });

      describe("#resubscribeConsumers", function() {
        beforeEach(function() {
          amqp.channels.forEach(function(stream) {
            stream._subscribeToQueue = sinon.stub().yields(null);
          });
        });

        afterEach(function() {
          amqp.channels.forEach(function(stream) {
            stream._subscribeToQueue.reset();
          });
        });

        it("attempts to resubscribe to the queue if the worker is unsubscribed", function(done) {
          amqp.resubscribeConsumers(function(err) {
            expect(err).to.not.be.ok();
            amqp.channels.forEach(function(stream) {
              expect(stream._subscribeToQueue.callCount).to.be(1);
            });
            done();
          });
        });

        it("doesn't attempt to subscribe to the queue if the queue is already subscribed", function(done) {
          //since we haven't initialized the streams in the test, let's manually say we've subscribed here
          amqp.channels.forEach(function(stream) {
            stream.subscribed = true;
          });
          amqp.resubscribeConsumers(function(err) {
            expect(err).to.not.be.ok();
            amqp.channels.forEach(function(stream) {
              expect(stream._subscribeToQueue.callCount).to.be(0);
            });
            done();
          });
        });
      });
    });

    describe('after network partition', function() {
      var rabbitmq = rewire('./');

      function stubConnection() {
        var subscriptionObj = {addCallback: sinon.stub().yields({})};

        var queueObj = new EventEmitter();
        queueObj.subscribe = sinon.stub().returns(subscriptionObj);

        var connectionObj = new EventEmitter();
        connectionObj.queue = sinon.stub().yields(queueObj);

        return connectionObj;
      }

      var createConnectionStub,
          connectionObj;

      beforeEach(function () {
        connectionObj = stubConnection();

        var AMQPStreams = rabbitmq.__get__('AMQPStreams');
        createConnectionStub =
          sinon.stub(AMQPStreams.prototype, "_createConnection").yields(null, connectionObj);
      });

      afterEach(function () {
        createConnectionStub.restore();
      });

      it("emits an error", function (done) {
        var onError = sinon.stub();

        var streams = rabbitmq.init(1, {
          connection: {},
          queue: {name: 'hi'}
        }, function(err, streams) {
          // listen for error events
          streams.on('error', onError);

          // simulate tcp socket dying
          connectionObj.emit('error', new Error('ECONNRESET'));

          sinon.assert.calledOnce(onError);
          done();
        });
      });

      it('closes streams', function(done) {
        var onError = sinon.stub(),
            onEnd = sinon.stub();

        var streams = rabbitmq.init(1, {
          connection: {},
          queue: {name: 'hi'}
        }, function(err, streams) {
          // suppress error events
          streams.on('error', onError);

          // pipe data through, and listen for 'end' events
          var channel = streams.channels[0];
          channel.source.pipe(channel.sink);
          channel.source.on('end', onEnd);

          // simulate tcp socket dying
          connectionObj.emit('error', new Error('ECONNRESET'));

          setTimeout(function() {
            sinon.assert.calledOnce(onEnd);
            done();
          }, 0);
        });
      });
    });
  });




  describe("AMQPStream", function() {

    var instance, connection, amqpResponseStub;

    beforeEach(function() {
      connection = new EventEmitter();
      connection.queue = sinon.stub();
      instance = new rabbitmq.AMQPStream(connection);
      amqpResponseStub = sinon.stub({
        acknowledge: function() {},
        reject: function() {}
      });
    });

    describe("AMQPStream.create", function() {

      var AMQPStreamMock;
      var initializeMethod;

      beforeEach(function () {
        var create = rabbitmq.AMQPStream.create;
        initializeMethod = sinon.stub();
        AMQPStreamMock = sinon.stub(rabbitmq, "AMQPStream");
        AMQPStreamMock.returns({ initialize: initializeMethod });
        AMQPStreamMock.create = create;
      });

      afterEach(function () {
        AMQPStreamMock.restore();
      });

      it("correctly increments the _totalWorkers count", function () {
        rabbitmq.AMQPStream.create();
        rabbitmq.AMQPStream.create();
        rabbitmq.AMQPStream.create();
        expect(rabbitmq.AMQPStream._totalWorkers).to.equal(3);
      });

      it("passes arguments to AMQPStream constructor and calls initialize with callback", function () {
        var options = {};
        var callback = sinon.stub();
        rabbitmq.AMQPStream.create(connection, options, callback);
        expect(AMQPStreamMock.args[0][0]).to.be(connection);
        expect(AMQPStreamMock.args[0][1]).to.be(options);
        expect(initializeMethod.callCount).to.be(1);
        expect(initializeMethod.args[0][0]).to.be(callback);
      });

    });

    describe("#initialize", function() {

      var instance, options, queue, connectToQueueStub, subscribeToQueueStub, streamifyQueueStub;

      beforeEach(function() {
        options = {
          name: "myStream",
          onError: sinon.stub()
        };
        queue = new EventEmitter();
        instance = new rabbitmq.AMQPStream(connection, options);
        connectToQueueStub = sinon.stub(rabbitmq.AMQPStream.prototype, "_connectToQueue");
        subscribeToQueueStub = sinon.stub(rabbitmq.AMQPStream.prototype, "_subscribeToQueue");
        streamifyQueueStub = sinon.stub(rabbitmq.AMQPStream.prototype, "_streamifyQueue");
      });

      afterEach(function() {
        connectToQueueStub.restore();
        subscribeToQueueStub.restore();
        streamifyQueueStub.restore();
      });

      it("errors without a name options", function() {
        var badInstance = new rabbitmq.AMQPStream({}, {});
        expect(badInstance.initialize.bind(badInstance)).to.throwError("You must provide a `name` to queueStream options object");
      });

      it("calls _connectToQueue with proper arguments", function() {
        instance.initialize();
        expect(connectToQueueStub.args[0][0]).to.be(options.name);
      });

      it("attaches error handler to queue", function() {
        connectToQueueStub.yields(null, queue);
        instance.initialize();
        expect(instance.__queue).to.be(queue);

        expect(options.onError.callCount).to.be(0);
        queue.emit("error");
        expect(options.onError.callCount).to.be(1);
      });

      it("calls _subscribeToQueue and calls _streamifyQueue on success", function() {
        connectToQueueStub.yields(null, queue);
        subscribeToQueueStub.yields(null);

        var cb = sinon.stub();
        instance.initialize(cb);
        expect(streamifyQueueStub.callCount).to.be(1);
        expect(streamifyQueueStub.args[0][0]).to.be(cb);
      });

    });

    describe("#_connectToQueue", function() {

      var cb;

      beforeEach(function() {
        cb = sinon.stub();
        // instance = new rabbitmq.AMQPStream(connection);
      });

      it("attaches error listener to connection", function() {
        instance._connectToQueue("myQueue", cb);
        var err = new Error("You have failed.");

        connection.emit("error", err);
        expect(cb.callCount).to.be(1);
        expect(cb.args[0][0]).to.be(err);

        expect(connection.emit.bind(connection, "error", err)).to.throwError("You have failed.");
      });

      it("queues connection to given queue", function() {
        var queue = {};
        connection.queue.yields(queue);

        instance._connectToQueue("myQueue", cb);

        var err = new Error("You have failed.");
        expect(cb.callCount).to.be(1);
        expect(cb.args[0][0]).to.be(null);
        expect(cb.args[0][1]).to.be(queue);

        // Since we have succesfully enqueued, we expect error listener to have detached,
        // and thus any emitted errors will not be caught.
        expect(connection.emit.bind(connection, "error", err)).to.throwError("You have failed.");
      });

      it("passes the `connection` option to the underlying driver for queue initialization", function() {
        instance = new rabbitmq.AMQPStream(connection, {connection: {passive: false}});
        instance.__connection = new EventEmitter();
        instance.__connection.queue = sinon.stub();
        instance._connectToQueue("myQueue", function() {});
        expect(instance.__connection.queue.args[0][1]).to.eql({passive: false});
      });

    });

    describe("#_handleIncomingMessage", function() {
      beforeEach(function() {
        //setup .source and .sink
        instance._streamifyQueue(function() {});
      });


      describe("when contentType === 'application/json'", function() {
        var message = {};
        var headers = {};
        var deliveryInfo = {
          parseError: new Error(),
          rawData: '{"malformed":',
          contentType: 'application/json',
          headers: {},
          deliveryMode: 1,
          queue: 'some-queue',
          deliveryTag: new Buffer("delivery-tag"),
          redelivered: false,
          exchange: '',
          routingKey: 'some-queue',
          consumerTag: 'consumer-tag'
        };


        it("automatically rejects any malformed message when no event listeners exist on 'parseError'", function (done) {
          instance.sink.on("rejected", function(msg) {
            done();
          });
          instance._handleIncomingMessage(null, {}, deliveryInfo, amqpResponseStub);
        });

        it("emits a `parseError` event on invalid JSON when there are event listeners", function (done) {
          instance.source.on("parseError", function(msg) {
            done();
          });
          instance._handleIncomingMessage(null, {}, deliveryInfo, amqpResponseStub);
        });
      });

    });

    describe("#_streamifyQueue", function() {

      var cb, writable, readable;

      beforeEach(function () {
        cb = sinon.stub();
        instance = new rabbitmq.AMQPStream(new EventEmitter());
        writable = new stream.Writable({objectMode: true});
        readable = new stream.Readable({objectMode: true});
      });

      describe("stream.source", function () {
        it("parses message, adds ackIndex, pushes downstream", function (done) {
          instance._waitForMessage = sinon.stub();
          instance._waitForMessage.onCall(0).yields({payload: {"something": "somethingElse"}, _meta: { ackIndex: 10 }});

          writable._write = function (message) {
            expect(message).to.eql({payload: {something: "somethingElse"}, _meta: {ackIndex: 10}});
            done();
          };

          instance._streamifyQueue(cb);
          instance.source.pipe(writable);
        });

        it("passes the `subscribe` option properly to the underlying driver", function () {
          instance = new rabbitmq.AMQPStream(connection, {subscribe: {prefetchCount: 100}});
          instance.__queue = {subscribe: sinon.stub()}; //.onFirstCall().returns({addCallback: function() {}});
          instance.__queue.subscribe.onFirstCall().returns({addCallback: function() {}});
          instance._subscribeToQueue();
          expect(instance.__queue.subscribe.args[0][0]).to.eql({ack: true, prefetchCount: 100});
        });
      });

      describe("stream.sink", function () {
        var goodMessage;

        beforeEach(function() {
          goodMessage = {_meta: {ackIndex: 1}};
        });

        it("emits a `formatError` when message._meta has no ackIndex", function (done) {
          var badMessage = {
            _meta: {}
          };
          readable._read = function () {
            this.push(badMessage);
          };

          instance._streamifyQueue(cb);
          instance.sink.on("formatError", function (err, msg) {
            expect(err).to.be.an(Error);
            expect(msg).to.be(badMessage);
            done();
          });
          readable.pipe(instance.sink);
        });

        it("emits an `ackError` event when no ack at ackIndex of outstandingAcks", function (done) {
          instance.__outstandingAcks = [undefined, {}];
          var badMessage = {
            _meta: {ackIndex: 0}
          };
          readable._read = function () {
            this.push(badMessage);
          };

          instance._streamifyQueue(cb);
          instance.sink.on("ackError", function (err, msg) {
            expect(err).to.be.an(Error);
            expect(msg).to.be(badMessage);
            done();
          });
          readable.pipe(instance.sink);
        });

        it("releases messages from queue when tagged with rabbitmq.RequeueMessage", function(done) {
          instance.__outstandingAcks = [
            undefined,
            amqpResponseStub
          ];
          readable._read = function () {
            this.push(rabbitmq.RequeueMessage(goodMessage));
          };
          instance._streamifyQueue(cb);
          instance.sink.on("requeued", function () {
            expect(amqpResponseStub.reject.callCount).to.be(1);
            expect(amqpResponseStub.reject.args[0][0]).to.be(true);
            expect(instance.__outstandingAcks[1]).to.be(null);
            done();
          });
          readable.pipe(instance.sink);
        });

        it("removes messages from queue when tagged with AMQPStream.RejectMessage", function(done) {
          instance.__outstandingAcks = [
            undefined,
            amqpResponseStub
          ];
          readable._read = function () {
            this.push(rabbitmq.RejectMessage(goodMessage));
          };
          instance._streamifyQueue(cb);
          instance.sink.on("rejected", function () {
            expect(amqpResponseStub.reject.callCount).to.be(1);
            expect(amqpResponseStub.reject.args[0][0]).to.be(false);
            expect(instance.__outstandingAcks[1]).to.be(null);
            done();
          });
          readable.pipe(instance.sink);
        });

        it("acknowledges ack, nulls it, and emits a `deleted` event", function (done) {
          instance.__outstandingAcks = [
            undefined,
            amqpResponseStub
          ];
          readable._read = function () {
            this.push(goodMessage);
          };

          instance._streamifyQueue(cb);
          instance.sink.on("acknowledged", function () {
            expect(amqpResponseStub.acknowledge.callCount).to.be(1);
            expect(amqpResponseStub.acknowledge.args[0][0]).to.be(false);
            expect(instance.__outstandingAcks[1]).to.be(null);
            done();
          });

          readable.pipe(instance.sink);
        });
      });

    });

    describe("#_waitForMessage", function() {

      var instance, cb;

      beforeEach(function() {
        cb = sinon.stub();
        instance = new rabbitmq.AMQPStream();
      });

      it("processes first message if items are enqueued", function(done) {
        instance.__pendingQueue = ["one", "two", "three"];
        instance._waitForMessage(cb);
        expect(cb.callCount).to.be(1);
        expect(cb.args[0][0]).to.be("one");
        expect(instance.__pendingQueue).to.eql(["two", "three"]);

        // Make sure we're not triggering the wait functionality.
        setTimeout(function() {
          expect(cb.callCount).to.be(1);
          done();
        }, 20);
      });

      it("waits for messages and begins processing when they appear", function(done) {
        instance._waitForMessage(cb);
        setTimeout(function() {
          expect(cb.callCount).to.be(0);

          // Put stuff into the queue and make sure it gets processed soon.
          instance.__pendingQueue = ["one", "two", "three"];
          setTimeout(function() {
            expect(cb.callCount).to.be(1);
            expect(cb.args[0][0]).to.be("one");
            expect(instance.__pendingQueue).to.eql(["two", "three"]);

            // Make sure we're not still waiting after receiving items.
            setTimeout(function() {
              expect(cb.callCount).to.be(1);
              done();
            }, 20);

          }, 20);

        }, 50);
      });

    });

    describe("#_insertAckIntoArray", function() {
      var instance;

      beforeEach(function() {
        instance = new rabbitmq.AMQPStream();
      });

      it("inserts ack into the first available spot", function() {
        instance.__outstandingAcks = ["ack1", undefined, "ack3"];
        var insertedIdx = instance._insertAckIntoArray("ack2");
        expect(insertedIdx).to.be(1);
        expect(instance.__outstandingAcks).to.eql(["ack1", "ack2", "ack3"]);

        var insertedIdx2 = instance._insertAckIntoArray("ack4");
        expect(insertedIdx2).to.be(3);
        expect(instance.__outstandingAcks).to.eql(["ack1", "ack2", "ack3", "ack4"]);
      });

    });

    describe("#unsubscribe", function() {

      var instance;

      beforeEach(function() {
        instance = new rabbitmq.AMQPStream();
      });

      it("calls unsubscribe on queue if currently subscribed", function(done) {
        var queue = {};

        var addCallback = sinon.stub();
        addCallback.yields();

        queue.unsubscribe = sinon.stub();
        queue.unsubscribe.returns({
          addCallback: addCallback
        });

        instance.__queue = queue;
        instance.subscribed = true;
        instance.__consumerTag = "myTag";

        instance.unsubscribe(function() {
          expect(instance.subscribed).to.be(false);

          expect(queue.unsubscribe.callCount).to.be(1);
          expect(queue.unsubscribe.args[0][0]).to.be("myTag");

          done();
        });
      });

      it("does nothing if already unsubscribed", function(done) {
        instance.subscribed = false;
        instance.unsubscribe(done);
      });
    });

    describe("#close", function() {
      var instance;
      beforeEach(function() {
        instance = new rabbitmq.AMQPStream();
        var queueMock = new EventEmitter();
        queueMock.close = sinon.spy();
        instance.__queue = queueMock;
      });

      it("calls this#close with the right consumer tag", function(done) {
        instance.__consumerTag = "consumerTag";
        instance.close(function(err) {
          expect(instance.__queue.close.callCount).to.be(1);
          expect(instance.__queue.close.getCall(0).args[0]).to.be(amqp.__consumerTag);
          done();
        });
        instance.__queue.emit("close");
      });

      it("returns on error if there was an issue with closing", function(done) {
        instance.close(function(err) {
          expect(err).to.be.an(Error);
          done();
        });
        instance.__queue.emit("error", new Error("Some error"));
      });

      it("does not return an error if the channel successfully closes", function(done) {
        instance.close(function(err) {
          expect(err).to.not.be.ok();
          done();
        });
        instance.__queue.emit("close");
      });
    });
  });

  describe("integration test", function() {
    /* Integration test by injecting fake amqp messages into _handleMessage. Cause
       The acks to queue the next message into the system.

    */
    var streamInstance;
    var message1 = {_id: "1"};
    var message2 = {_id: "2"};
    var message3 = {_id: "3"};
    var payloads = [message1, message2, message3];

    var ackStub = function(instance) {
      return {
        acknowledge: function() {
          if(payloads.length) {
            setTimeout(function() {
              injectNewMessage(instance);
            }, 10);
          }
        }
      };
    };

    var injectNewMessage = function(instance) {
      instance._handleIncomingMessage(payloads.shift(), {}, {contentType: "application/json"}, ackStub(instance));
    };

    before(function(done) {
      var connection = new EventEmitter();
      streamInstance = new rabbitmq.AMQPStream(connection);
      //setup .source and .sink properties
      streamInstance._streamifyQueue(done);
    });

    it("pipes all outstanding messages received by rabbit downstream when properly acked", function(done) {
      var receivedMessages = [];
      var collector = new stream.Transform({objectMode: true});
      collector._transform = function(obj, enc, next) {
        receivedMessages.push(obj);
        if(receivedMessages.length === 3) {
          expect(receivedMessages).to.eql([message1, message2, message3].map(
            function(m) {
              return {payload: m, _meta: {ackIndex: 0}};
            }
          ));
          done();
        }
        this.push(obj);
        next();
      };
      streamInstance.source.pipe(collector).pipe(streamInstance.sink);
      injectNewMessage(streamInstance);
    });

  });
});


