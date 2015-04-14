# Last-Value Queues

Last-Value queues are special queues which discard any messages when a
newer message with the same value for a well-defined Last-Value property
is put in the queue. In other words, a Last-Value queue only retains the
last value.

A typical example for Last-Value queue is for stock prices, where you
are only interested by the latest value for a particular stock.

## Configuring Last-Value Queues

Last-value queues are defined in the address-setting configuration:

    <address-setting match="jms.queue.lastValueQueue">
       <last-value-queue>true</last-value-queue>
    </address-setting>

By default, `last-value-queue` is false. Address wildcards can be used
to configure Last-Value queues for a set of addresses (see [here](wildcard-syntax.md)).

## Using Last-Value Property

The property name used to identify the last value is `"_AMQ_LVQ_NAME"`
(or the constant `Message.HDR_LAST_VALUE_NAME` from the Core API).

For example, if two messages with the same value for the Last-Value
property are sent to a Last-Value queue, only the latest message will be
kept in the queue:

``` java
// send 1st message with Last-Value property set to STOCK_NAME
TextMessage message = session.createTextMessage("1st message with Last-Value property set");
message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
producer.send(message);

// send 2nd message with Last-Value property set to STOCK_NAME
message = session.createTextMessage("2nd message with Last-Value property set");
message.setStringProperty("_AMQ_LVQ_NAME", "STOCK_NAME");
producer.send(message);

...

// only the 2nd message will be received: it is the latest with
// the Last-Value property set
TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
System.out.format("Received message: %s\n", messageReceived.getText());
```

## Example

See the [examples](examples.md) chapter for an example which shows how last value queues are configured
and used with JMS.
