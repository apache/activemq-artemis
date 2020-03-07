# Spring Integration

Apache ActiveMQ Artemis provides a simple bootstrap class,
`org.apache.activemq.artemis.integration.spring.SpringJmsBootstrap`, for
integration with Spring. To use it, you configure Apache ActiveMQ Artemis as
you always would, through its various configuration files like `broker.xml`.

The `SpringJmsBootstrap` class extends the EmbeddedJMS class talked about in
[embedding ActiveMQ](embedding-activemq.md) and the same defaults and
configuration options apply. See the javadocs for more details on other
properties of the bean class.

## Example

See the [Spring Integration Example](examples.md#spring-integration) for a
demonstration of how this can work.
