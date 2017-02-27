Connectors
=====================================

After broker is started, you'll want to connect your clients to it. So, let's start with comparing ActiveMQ and Artemis configurations in area of client connectors. In ActiveMQ terminology, they are called *transport connectors*, and the default configuration looks something like this (in `conf/activemq.xml`).

```xml
<transportConnectors>
    <transportConnector name="openwire" uri="tcp://0.0.0.0:61616?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
    <transportConnector name="amqp" uri="amqp://0.0.0.0:5672?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
    <transportConnector name="stomp" uri="stomp://0.0.0.0:61613?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
    <transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
    <transportConnector name="ws" uri="ws://0.0.0.0:61614?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
</transportConnectors>
```
        
In Artemis, client connectors are called *acceptors* and they are configured in `etc/broker.xml` like this
```xml
<acceptors>
    <acceptor name="artemis">tcp://0.0.0.0:61616?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE</acceptor>
    <acceptor name="amqp">tcp://0.0.0.0:5672?protocols=AMQP</acceptor>
    <acceptor name="stomp">tcp://0.0.0.0:61613?protocols=STOMP</acceptor>
    <acceptor name="hornetq">tcp://0.0.0.0:5445?protocols=HORNETQ,STOMP</acceptor>
    <acceptor name="mqtt">tcp://0.0.0.0:1883?protocols=MQTT</acceptor>
</acceptors>        
```	
As you can notice the syntax is very similar, but there are still some differences that we need to understand. First, as we said earlier, there's no notion of blocking	and non-blocking (nio) transport in Artemis, so you should treat everything as non-blocking. Also, in Artemis the low level transport is distinct from the actual messaging protocol (like AMQP or MQTT) used on top of it. One acceptor can handle multiple messaging protocols on the same port. By default, all protocols are accepted on the single port, but you can restrict this using the `protocols=X,Y` uri attribute pattern as shown in the example above.
  
Besides *tcp* network protocol, Artemis support *InVm* and *Web Socket* transports. The *InVm* transport is similar to ActiveMQ's *vm* transport and is used to connect clients to the embedded broker. The difference is that you can use any messaging protocol on top of *InVm* transport in Artemis, while *vm* transport in ActiveMQ is tied to OpenWire.
  
One of the advantages of using Netty for IO layer, is that Web Sockets are supported out of the box. So, there's no need for the separate *ws* transport like in ActiveMQ, the *tcp* (Netty) acceptor in Artemis will detect Web Socket clients and handle them accordingly.  

To summarize this topic, here's a table that shows you how to migrate your ActiveMQ transport connectors to the Artemis acceptors 

| ActiveMQ  | Artemis (options in the acceptor URL) |
|---|---|
| OpenWire  | protocols=OpenWire (version 10+)  |
| NIO  | -  |
| AMQP  | protocols=AMQP  |
| STOMP | protocols=STOMP  |
| VM (OpenWire only)  | InVM (all protocols, peer to tcp)  |
| HTTP (OpenWire-based)  | -  |
| MQTT  | protocols=MQTT  |
| WebSocket (STOMP and MQTT) | handled by tcp (all protocols)|


