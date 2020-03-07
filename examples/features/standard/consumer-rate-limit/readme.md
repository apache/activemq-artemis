# JMS Message Consumer Rate Limiting

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

With ActiveMQ Artemis you can specify a maximum consume rate at which a JMS MessageConsumer will consume messages. This can be specified when creating or configuring the connection factory. See `jndi.properties`.

If this value is specified then ActiveMQ Artemis will ensure that messages are never consumed at a rate higher than the specified rate. This is a form of consumer _throttling_.

## Example step-by-step

In this example we specify a `consumer-max-rate` of `10` messages per second in the `jndi.properties` file when configuring the connection factory:

    connectionFactory.ConnectionFactory=tcp://localhost:61616?consumerMaxRate=10

We then simply consume as many messages as we can in 10 seconds and note how many messages are actually consumed.

We note that the number of messages consumed per second never exceeds the specified value of `10` messages per second.