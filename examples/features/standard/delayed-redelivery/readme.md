# Delayed Redelivery Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates how ActiveMQ Artemis can be configured to provide a delayed redelivery in the case where a message needs to be redelivered.

Delaying redelivery can often be useful in the case that clients regularly fail or roll-back. Without a delayed redelivery, the system can get into a "thrashing" state, with delivery being attempted, the client rolling back, and delivery being re-attempted ad infinitum in quick succession, using up valuable CPU and network resources.

Re-delivery occurs when the session is closed with unacknowledged messages. The unacknowledged messages will be redelivered.

By providing a redelivery delay, it can be specified that a delay of, say, 10 seconds is implemented between rollback and redelivery. The specific delay is configurable on both a global and per destination level, by using wild-card matching on the address settings.

## Example setup

Redelivery delay is specified in the configuration file [broker.xml](src/main/resources/activemq/server0/broker.xml):

In this example we set the redelivery delay to 5 seconds for the specific example queue. We could set redelivery delay on multiple queues by specifying a wild-card in the match, e.g. `match="jms.#"` would apply the settings to all JMS queues and topics.

We then consume a message in a transacted session, and rollback, and note that the message is not redelivered until after 5 seconds.

    <address-setting match="exampleQueue">
       <redelivery-delay>5000</redelivery-delay>
    </address-setting>
