# OpenWire

Apache ActiveMQ Artemis supports the
[OpenWire](http://activemq.apache.org/openwire.html) protocol so that an Apache
ActiveMQ 5.x JMS client can talk directly to an Apache ActiveMQ Artemis server.
By default there is an `acceptor` configured to accept OpenWire connections on
port `61616`.

See the general [Protocols and Interoperability](protocols-interoperability.md)
chapter for details on configuring an `acceptor` for OpenWire.

Refer to the OpenWire examples for a look at this functionality in action.

## Connection Monitoring

OpenWire has a few parameters to control how each connection is monitored, they
are:

- `maxInactivityDuration`

  It specifies the time (milliseconds) after which the connection is closed by
  the broker if no data was received.  Default value is 30000.

- `maxInactivityDurationInitalDelay`

  It specifies the maximum delay (milliseconds) before inactivity monitoring is
  started on the connection. It can be useful if a broker is under load with many
  connections being created concurrently. Default value is 10000.

- `useInactivityMonitor`

  A value of false disables the InactivityMonitor completely and connections
  will never time out. By default it is enabled. On broker side you don't neet
  set this. Instead you can set the connection-ttl to -1.

- `useKeepAlive`

  Indicates whether to send a KeepAliveInfo on an idle connection to prevent it
  from timing out. Enabled by default.  Disabling the keep alive will still make
  connections time out if no data was received on the connection for the
  specified amount of time.

Note at the beginning the InactivityMonitor negotiates the appropriate
`maxInactivityDuration` and `maxInactivityDurationInitalDelay`. The shortest
duration is taken for the connection.

Fore more details please see [ActiveMQ
InactivityMonitor](http://activemq.apache.org/activemq-inactivitymonitor.html).

## Disable/Enable Advisories

By default, advisory topics ([ActiveMQ
Advisory](http://activemq.apache.org/advisory-message.html)) are created in
order to send certain type of advisory messages to listening clients. As a
result, advisory addresses and queues will be displayed on the management
console, along with user deployed addresses and queues. This sometimes cause
confusion because the advisory objects are internally managed without user
being aware of them. In addition, users may not want the advisory topics at all
(they cause extra resources and performance penalty) and it is convenient to
disable them at all from the broker side.

The protocol provides two parameters to control advisory behaviors on the
broker side.

- `supportAdvisory`

  Indicates whether the broker supports advisory messages. If the value is true,
  advisory addresses/queues will be created.  If the value is false, no advisory
  addresses/queues are created. Default value is `true`. 

- `suppressInternalManagementObjects`

  Indicates whether advisory addresses/queues, if any, will be registered to
  management service (e.g. JMX registry). If set to true, no advisory
  addresses/queues will be registered. If set to false, those are registered and
  will be displayed on the management console. Default value is `true`.

The two parameters are configured on an OpenWire `acceptor`, e.g.:

```xml
<acceptor name="artemis">tcp://localhost:61616?protocols=OPENWIRE;supportAdvisory=true;suppressInternalManagementObjects=false</acceptor>
```

## Virtual Topic Consumer Destination Translation

For existing OpenWire consumers of virtual topic destinations it is possible to
configure a mapping function that will translate the virtual topic consumer
destination into a FQQN address. This address will then represents the consumer as a
multicast binding to an address representing the virtual topic. 

The configuration string list property `virtualTopicConsumerWildcards` has parts
separated by a `;`. The first is the classic style destination filter that
identifies the destination as belonging to a virtual topic. The second
identifies the number of `paths` that identify the consumer queue such that it
can be parsed from the destination. Any subsequent parts are additional configuration
parameters for that mapping.

For example, the default virtual topic with consumer prefix of `Consumer.*.`, would require a
`virtualTopicConsumerWildcards` filter of `Consumer.*.>;2`. As a url parameter
this transforms to `Consumer.*.%3E%3B2` when the url significant characters
`>;` are escaped with their hex code points. In an `acceptor` url it would be:

```xml
<acceptor name="artemis">tcp://localhost:61616?protocols=OPENWIRE;virtualTopicConsumerWildcards=Consumer.*.%3E%3B2</acceptor>
```

This will translate `Consumer.A.VirtualTopic.Orders` into a FQQN of
`VirtualTopic.Orders::Consumer.A.VirtualTopic.Orders` using the int component `2` of the
configuration to identify the consumer queue as the first two paths of the
destination. `virtualTopicConsumerWildcards` is multi valued using a `,`
separator.

### selectorAware
The mappings support an optional parameter, `selectorAware` which when true, transfers any selector information from the
OpenWire consumer into a queue filter of any auto-created subscription queue. Note: the selector/filter is persisted with
the queue binding in the normal way, such that it works independent of connected consumers.

Please see Virtual Topic Mapping example contained in the OpenWire
[examples](examples.md).