# Management Notification Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how to receive management notifications from ActiveMQ Artemis using JMS Messages.

ActiveMQ Artemis servers emit management notifications when events of interest occur (consumers are created or closed, destinations are created or deleted, security authentication fails, etc.).
These notifications can be received either by using JMX (see "jmx" example) or by receiving JMS Messages from a well-known destination.

This example will setup a JMS MessageListener to receive management notifications. We will also perform normal JMS operations to see the kind of notifications they trigger.

## Example configuration

ActiveMQ Artemis can configured to send JMS messages when management notifications are emitted on the server.

By default, the management name is called `activemq.notifications` but this can be configured in [broker.xml](server0/broker.xml). For this example, we will set it to `jms.topic.notificationsTopic` to be able to receive notifications from a JMS Topic.

    <management-notification-address>jms.topic.notificationsTopic</management-notification-address>

The notification queue requires permission to create/delete temporary queues and consume messages. This is also configured in [broker.xml](server0/broker.xml)

    <security-setting match="jms.topic.notificationsTopic">
       <permission type="consume" roles="guest"/>
       <permission type="createNonDurableQueue" roles="guest"/>
       <permission type="deleteNonDurableQueue" roles="guest"/>
    </security-setting>