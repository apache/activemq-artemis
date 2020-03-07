# JMS Pre-Acknowledge Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

Standard JMS supports three acknowledgement modes: AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, and DUPS_OK_ACKNOWLEDGE. For a full description on these modes please consult the JMS specification, or any JMS tutorial.

All of these standard modes involve sending acknowledgements from the client to the server. However in some cases, you really don't mind losing messages in event of failure, so it would make sense to acknowledge the message on the broker **before** delivering it to the client.

By acknowledging the message before sending to the client, you can avoid extra network traffic and CPU work done in sending acknowledgements from client to server.

The down-side of acknowledging on the broker before delivery, is that if the system crashes after acknowledging the message, but before the message has been received by the client, then, on recovery, that message will be lost. This makes pre-acknowledgement not appropriate for all use cases, but it is very useful for some use-cases when you can cope with such loss of messages

An example of a use-case where it might be a good idea to use pre-acknowledge, is for stock price update messages. With these messages it might be ok to lose a message in event of crash, since the next price update message will arrive soon, overriding the previous price.

In order to use pre-acknowledge functionality with ActiveMQ Artemis the session has to be created with a special, ActiveMQ Artemis specific acknowledgement mode, given by the value of `ActiveMQJMSConstants.PRE_ACKNOWLEDGE`.