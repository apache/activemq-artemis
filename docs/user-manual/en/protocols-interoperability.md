# Protocols and Interoperability

Apache ActiveMQ Artemis has a powerful & flexible core which provides a foundation upon which other protocols can be
implemented. Each protocol implementation translates the ideas of its specific protocol onto this core.

The broker ships with a client implementation which interacts directly with this core. It uses what's called the ["core"
API](core.md), and it communicates over the network using the "core" protocol.

## Supported Protocols & APIs

The broker has a pluggable protocol architecture.  Protocol plugins come in the form of protocol modules.  Each protocol 
module is included on the broker's class path and loaded by the broker at boot time. The broker ships with 5 protocol 
modules out of the box. The 5 modules offer support for the following protocols:

- [AMQP](amqp.md)
- [OpenWire](openwire.md)
- [MQTT](mqtt.md)
- [STOMP](stomp.md)
- HornetQ

#### APIs and Other Interfaces

Although JMS is a standardized API, it does not define a network protocol. The [ActiveMQ Artemis JMS 2.0 client](using-jms.md) 
is implemented on top of the core protocol. We also provide a [client-side JNDI implementation](using-jms.md#jndi).

The broker also ships with a [REST messaging interface](rest.md) (not to be confused with the REST management API
provided via our integration with Jolokia).

## Configuring Acceptors

In order to make use of a particular protocol, a transport must be configured with the desired protocol enabled.  There
is a whole section on configuring transports that can be found [here](configuring-transports.md).

The default configuration shipped with the ActiveMQ Artemis distribution comes with a number of acceptors already
defined, one for each of the above protocols plus a generic acceptor that supports all protocols.  To enable 
protocols on a particular acceptor simply add the `protocols` url parameter to the acceptor url where the value is one
or more protocols (separated by commas). If the `protocols` parameter is omitted from the url **all** protocols are 
enabled.

- The following example enables only MQTT on port 1883
```xml
<acceptors>
   <acceptor>tcp://localhost:1883?protocols=MQTT</acceptor>
</acceptors>
```

- The following example enables MQTT and AMQP on port 1883
```xml
<acceptors>
   <acceptor>tcp://localhost:5672?protocols=MQTT,AMQP</acceptor>
</acceptors>
```

- The following example enables **all** protocols on `61616`:
```xml
<acceptors>
   <acceptor>tcp://localhost:61616</acceptor>
</acceptors>
```

Here are the supported protocols and their corresponding value used in the `protocols` url parameter.

Protocol|`protocols` value
---|---
Core (Artemis & HornetQ native)|`CORE`
OpenWire (5.x native)|`OPENWIRE`
AMQP|`AMQP`
MQTT|`MQTT`
STOMP|`STOMP`