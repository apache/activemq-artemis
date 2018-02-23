# Last-Value Queues

Last-Value queues are special queues which discard any messages when a
newer message with the same value for a well-defined Last-Value property
is put in the queue. In other words, a Last-Value queue only retains the
last value.

A typical example for Last-Value queue is for stock prices, where you
are only interested by the latest value for a particular stock.

## Configuring Last-Value Queues

Last-Value queues can be pre configured at the address queue level

```xml
<configuration ...>
  <core ...>
    ...
    <address name="foo.bar">
      <multicast>
        <queue name="orders1" last-value="true"/>
      </multicast>
    </address>
  </core>
</configuration>
```

Specified on creating a Queue by using the CORE api specifying the parameter `lastValue` to `true`. 

Or on auto-create when using the JMS Client by using address parameters when creating the destination used by the consumer.

    Queue queue = session.createQueue("my.destination.name?last-value=true");
    Topic topic = session.createTopic("my.destination.name?last-value=true");

Also the default for all queues under and address can be defaulted using the address-setting configuration:

    <address-setting match="lastValueQueue">
       <default-last-value-queue>true</default-last-value-queue>
    </address-setting>

By default, `default-last-value-queue` is false. 
Address wildcards can be used to configure Last-Value queues 
for a set of addresses (see [here](wildcard-syntax.md)).

Note that address-setting `last-value-queue` config is deprecated, please use `default-last-value-queue` instead.

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
