# No Consumer Buffering Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

By default, ActiveMQ Artemis consumers buffer messages from the broker in a client side buffer before actual delivery actually occurs.

This improves performance since otherwise every time you called receive() or had processed the last message in a MessageListener onMessage() method, the ActiveMQ Artemis client would have to go the broker to request the next message involving a network round trip for every message reducing performance.

Therefore, by default, ActiveMQ Artemis pre-fetches messages into a buffer on each consumer. The total maximum size of messages in bytes that will be buffered on each consumer is determined by the `consumerWindowSize` parameter on the connection URL.

In some cases it is not desirable to buffer any messages on the client side consumer.

An example would be an order queue which had multiple consumers that processed orders from the queue. Each order takes a significant time to process, but each one should be processed in a timely fashion.

If orders were buffered in each consumer, and a new consumer was added that consumer would not be able to process orders which were already in the client side buffer of another consumer.

To turn off client side buffering of messages, set `consumerWindowSize` to zero.

With ActiveMQ Artemis you can specify a maximum consume rate at which a JMS MessageConsumer will consume messages. This can be specified when configuring the connection URL.

## Example step-by-step

In this example we specify a `consumerWindowSize` of `0` bytes on the connection URL in `jndi.properties`.

We create a consumer on a queue and send 10 messages to it. We then create another consumer on the same queue.

We then consume messages from each consumer in a semi-random order. We note that the messages are consumed in the order they were sent.

If the messages had been buffered in each consumer they would not be available to be consumed in an order determined after delivery.