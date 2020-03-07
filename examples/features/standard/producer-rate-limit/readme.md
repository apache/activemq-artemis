# JMS Message Producer Rate Limiting

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

With ActiveMQ Artemis you can specify a maximum send rate at which a JMS MessageProducer will send messages. This can be specified when creating or deploying the connection factory. See `activemq-jms.xml`

If this value is specified then ActiveMQ Artemis will ensure that messages are never produced at a rate higher than specified. This is a form of producer _throttling_.

## Example step-by-step

In this example we specify a `producerMaxRate` of `50` messages per second on the connection URL.

We then simply send as many messages as we can in 10 seconds and note how many messages are actually sent.

We note that the number of messages sent per second never exceeds the specified value of `50` messages per second.