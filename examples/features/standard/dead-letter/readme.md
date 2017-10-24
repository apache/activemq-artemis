# Dead Letter Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to define and deal with dead letter messages.

Messages can be delivered unsuccessfully (e.g. if the transacted session used to consume them is rolled back). Such a message goes back to the JMS destination ready to be redelivered. However, this means it is possible for a message to be delivered again and again without any success and remain in the destination, clogging the system.

To prevent this, messaging systems define dead letter messages: after a specified unsuccessful delivery attempts, the message is removed from the destination and instead routed to a _dead letter address_ where they can be consumed for further investigation.

The example will show how to configure ActiveMQ Artemis to route a message to a dead letter address after 3 unsuccessful delivery attempts.

The example will send 1 message to a queue. We will deliver the message 3 times and rollback the session every time.

On the 4th attempt, there won't be any message to consume: it will have been moved to a _dead letter address_.

We will then consume this dead letter message.

## Example setup

_Dead letter addresses_ and _maximum delivery attempts_ are defined in the configuration file [broker.xml](src/main/resources/activemq/server0/broker.xml):

    <address-setting match="exampleQueue">
       <dead-letter-address>deadLetterQueue</dead-letter-address>
       <max-delivery-attempts>3</max-delivery-attempts>
    </address-setting>

This configuration will moved dead letter messages from `exampleQueue` to the `deadLetterQueue`.

ActiveMQ Artemis allows to specify either an address or a queue. In this example, we will use a queue to hold the dead letter messages.

The maximum attempts of delivery is `3`. Once this figure is reached, a message is considered a dead letter message and is moved to the `deadLetterQueue`.