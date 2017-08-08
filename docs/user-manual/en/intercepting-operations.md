# Intercepting Operations

Apache ActiveMQ Artemis supports *interceptors* to intercept packets entering and
exiting the server. Incoming and outgoing interceptors are be called for
any packet entering or exiting the server respectively. This allows
custom code to be executed, e.g. for auditing packets, filtering or
other reasons. Interceptors can change the packets they intercept. This
makes interceptors powerful, but also potentially dangerous.

## Implementing The Interceptors

All interceptors are protocol specific.

An interceptor for the core protocol must implement the interface `Interceptor`:

``` java
package org.apache.activemq.artemis.api.core.interceptor;

public interface Interceptor
{
   boolean intercept(Packet packet, RemotingConnection connection) throws ActiveMQException;
}
```

For stomp protocol an interceptor must implement the interface `StompFrameInterceptor`:

``` java
package org.apache.activemq.artemis.core.protocol.stomp;

public interface StompFrameInterceptor extends BaseInterceptor<StompFrame>
{
   boolean intercept(StompFrame stompFrame, RemotingConnection connection);
}
```

Likewise for MQTT protocol, an interceptor must implement the interface `MQTTInterceptor`:
 
``` java
package org.apache.activemq.artemis.core.protocol.mqtt;

public interface MQTTInterceptor extends BaseInterceptor<MqttMessage>
{
    boolean intercept(MqttMessage mqttMessage, RemotingConnection connection);
}
```

The returned boolean value is important:

-   if `true` is returned, the process continues normally

-   if `false` is returned, the process is aborted, no other
    interceptors will be called and the packet will not be processed
    further by the server.

## Configuring The Interceptors

Both incoming and outgoing interceptors are configured in
`broker.xml`:

    <remoting-incoming-interceptors>
       <class-name>org.apache.activemq.artemis.jms.example.LoginInterceptor</class-name>
       <class-name>org.apache.activemq.artemis.jms.example.AdditionalPropertyInterceptor</class-name>
    </remoting-incoming-interceptors>

    <remoting-outgoing-interceptors>
       <class-name>org.apache.activemq.artemis.jms.example.LogoutInterceptor</class-name>
       <class-name>org.apache.activemq.artemis.jms.example.AdditionalPropertyInterceptor</class-name>
    </remoting-outgoing-interceptors>

The interceptors classes (and their dependencies) must be added to the
server classpath to be properly instantiated and called.

## Interceptors on the Client Side

The interceptors can also be run on the Apache ActiveMQ Artemit client side to intercept packets
either sent by the client to the server or by the server to the client.
This is done by adding the interceptor to the `ServerLocator` with the
`addIncomingInterceptor(Interceptor)` or
`addOutgoingInterceptor(Interceptor)` methods.

As noted above, if an interceptor returns `false` then the sending of
the packet is aborted which means that no other interceptors are be
called and the packet is not be processed further by the client.
Typically this process happens transparently to the client (i.e. it has
no idea if a packet was aborted or not). However, in the case of an
outgoing packet that is sent in a `blocking` fashion a
`ActiveMQException` will be thrown to the caller. The exception is
thrown because blocking sends provide reliability and it is considered
an error for them not to succeed. `Blocking` sends occurs when, for
example, an application invokes `setBlockOnNonDurableSend(true)` or
`setBlockOnDurableSend(true)` on its `ServerLocator` or if an
application is using a JMS connection factory retrieved from JNDI that
has either `block-on-durable-send` or `block-on-non-durable-send` set to
`true`. Blocking is also used for packets dealing with transactions
(e.g. commit, roll-back, etc.). The `ActiveMQException` thrown will
contain the name of the interceptor that returned false.

As on the server, the client interceptor classes (and their
dependencies) must be added to the classpath to be properly instantiated
and invoked.

## Example

See [the examples chapter](examples.md) for an example which shows how to use interceptors to add
properties to a message on the server.
