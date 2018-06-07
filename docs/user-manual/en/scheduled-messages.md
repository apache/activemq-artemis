# Scheduled Messages

Scheduled messages differ from normal messages in that they won't be
delivered until a specified time in the future, at the earliest.

To do this, a special property is set on the message before sending it.

## Scheduled Delivery Property

The property name used to identify a scheduled message is
`"_AMQ_SCHED_DELIVERY"` (or the constant
`Message.HDR_SCHEDULED_DELIVERY_TIME`).

The specified value must be a positive `long` corresponding to the time
the message must be delivered (in milliseconds). An example of sending a
scheduled message using the JMS API is as follows.

```java
TextMessage message = session.createTextMessage("This is a scheduled message message which will be delivered in 5 sec.");
message.setLongProperty("_AMQ_SCHED_DELIVERY", System.currentTimeMillis() + 5000);
producer.send(message);

...

// message will not be received immediately but 5 seconds later
TextMessage messageReceived = (TextMessage) consumer.receive();
```
Scheduled messages can also be sent using the core API, by setting the
same property on the core message before sending.

## Example

See the [Scheduled Message Example](examples.md#scheduled-message) which shows how scheduled messages can be used with
JMS.
