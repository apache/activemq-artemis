# Vert.x Integration

[Vert.x](http://vertx.io/) is a lightweight, high performance
application platform for the JVM that's designed for modern mobile, web,
and enterprise applications. Vert.x provides a distributed event bus
that allows messages to be sent across vert.x instances and clients. You
can now redirect and persist any vert.x messages to Apache ActiveMQ Artemis and route
those messages to a specified vertx address by configuring Apache ActiveMQ Artemis
vertx incoming and outgoing vertx connector services.

## Configuring a Vertx Incoming Connector Service

Vertx Incoming Connector services receive messages from vertx event bus
and route them to an Apache ActiveMQ Artemis queue. Such a service can be configured as
follows:

    <connector-service name="vertx-incoming-connector">
    <factory-class>org.apache.activemq.integration.vertx.VertxIncomingConnectorServiceFactory</factory-class>
    <param key="host" value="127.0.0.1"/>
    <param key="port" value="0"/>
    <param key="queue" value="jms.queue.vertxQueue"/>
    <param key="vertx-address" value="vertx.in.eventaddress"/>
    </connector-service>


Shown are the required params for the connector service:

-   `queue`. The name of the Apache ActiveMQ Artemis queue to send message to.

As well as these required parameters there are the following optional
parameters

-   `host`. The host name on which the vertx target container is
    running. Default is localhost.

-   `port`. The port number to which the target vertx listens. Default
    is zero.

-   `quorum-size`. The quorum size of the target vertx instance.

-   `ha-group`. The name of the ha-group of target vertx instance.
    Default is `activemq`.

-   `vertx-address`. The vertx address to listen to. default is
    `org.apache.activemq`.

## Configuring a Vertx Outgoing Connector Service

Vertx Outgoing Connector services fetch vertx messages from a ActiveMQ
queue and put them to vertx event bus. Such a service can be configured
as follows:

    <connector-service name="vertx-outgoing-connector">
    <factory-class>org.apache.activemq.integration.vertx.VertxOutgoingConnectorServiceFactory</factory-class>
    <param key="host" value="127.0.0.1"/>
    <param key="port" value="0"/>
    <param key="queue" value="jms.queue.vertxQueue"/>
    <param key="vertx-address" value="vertx.out.eventaddress"/>
    <param key="publish" value="true"/>
    </connector-service>


Shown are the required params for the connector service:

-   `queue`. The name of the Apache ActiveMQ Artemis queue to fetch message from.

As well as these required paramaters there are the following optional
parameters

-   `host`. The host name on which the vertx target container is
    running. Default is localhost.

-   `port`. The port number to which the target vertx listens. Default
    is zero.

-   `quorum-size`. The quorum size of the target vertx instance.

-   `ha-group`. The name of the ha-group of target vertx instance.
    Default is `activemq`.

-   `vertx-address`. The vertx address to put messages to. default is
    org.apache.activemq.

-   `publish`. How messages is sent to vertx event bus. "true" means
    using publish style. "false" means using send style. Default is
    false.


