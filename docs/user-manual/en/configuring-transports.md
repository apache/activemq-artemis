# Configuring the Transport

ActiveMQ has a fully pluggable and highly flexible transport layer and
defines its own Service Provider Interface (SPI) to make plugging in a
new transport provider relatively straightforward.

In this chapter we'll describe the concepts required for understanding
ActiveMQ transports and where and how they're configured.

## Understanding Acceptors

One of the most important concepts in ActiveMQ transports is the
*acceptor*. Let's dive straight in and take a look at an acceptor
defined in xml in the configuration file `activemq-configuration.xml`.

    <acceptors>
       <acceptor name="netty">
          <factory-class>
             org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory
          </factory-class>
          <param key="port" value="5446"/>
       </acceptor>
    </acceptors>

Acceptors are always defined inside an `acceptors` element. There can be
one or more acceptors defined in the `acceptors` element. There's no
upper limit to the number of acceptors per server.

Each acceptor defines a way in which connections can be made to the
ActiveMQ server.

In the above example we're defining an acceptor that uses
[Netty](http://netty.io/) to listen for connections at port
`5446`.

The `acceptor` element contains a sub-element `factory-class`, this
element defines the factory used to create acceptor instances. In this
case we're using Netty to listen for connections so we use the Netty
implementation of an `AcceptorFactory` to do this. Basically, the
`factory-class` element determines which pluggable transport we're going
to use to do the actual listening.

The `acceptor` element can also be configured with zero or more `param`
sub-elements. Each `param` element defines a key-value pair. These
key-value pairs are used to configure the specific transport, the set of
valid key-value pairs depends on the specific transport be used and are
passed straight through to the underlying transport.

Examples of key-value pairs for a particular transport would be, say, to
configure the IP address to bind to, or the port to listen at.

Note that unlike versions before 2.4 an Acceptor can now support
multiple protocols. By default this will be all available protocols but
can be limited by either the now deprecated `protocol` param or by
setting a comma seperated list to the newly added `protocols` parameter.

## Understanding Connectors

Whereas acceptors are used on the server to define how we accept
connections, connectors are used by a client to define how it connects
to a server.

Let's look at a connector defined in our `activemq-configuration.xml`
file:

    <connectors>
       <connector name="netty">
          <factory-class>
             org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory
          </factory-class>
          <param key="port" value="5446"/>
       </connector>
    </connectors>

Connectors can be defined inside a `connectors` element. There can be
one or more connectors defined in the `connectors` element. There's no
upper limit to the number of connectors per server.

You make ask yourself, if connectors are used by the *client* to make
connections then why are they defined on the *server*? There are a
couple of reasons for this:

-   Sometimes the server acts as a client itself when it connects to
    another server, for example when one server is bridged to another,
    or when a server takes part in a cluster. In this cases the server
    needs to know how to connect to other servers. That's defined by
    *connectors*.

-   If you're using JMS and you're using JNDI on the client to look up
    your JMS connection factory instances then when creating the
    `ActiveMQConnectionFactory` it needs to know what server that
    connection factory will create connections to.

    That's defined by the `java.naming.provider.url` element in the JNDI
    context environment, e.g. `jndi.properties`. Behind the scenes, the
    `org.apache.activemq.jndi.ActiveMQInitialContextFactory` uses the
    `java.naming.provider.url` to construct the transport. Here's a
    simple example:

        java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory
        java.naming.provider.url=tcp://myhost:5445

## Configuring the transport directly from the client side.

How do we configure a core `ClientSessionFactory` with the information
that it needs to connect with a server?

Connectors are also used indirectly when directly configuring a core
`ClientSessionFactory` to directly talk to a server. Although in this
case there's no need to define such a connector in the server side
configuration, instead we just create the parameters and tell the
`ClientSessionFactory` which connector factory to use.

Here's an example of creating a `ClientSessionFactory` which will
connect directly to the acceptor we defined earlier in this chapter, it
uses the standard Netty TCP transport and will try and connect on port
5446 to localhost (default):

``` java
Map<String, Object> connectionParams = new HashMap<String, Object>();

connectionParams.put(org.apache.activemq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    5446);

TransportConfiguration transportConfiguration =
    new TransportConfiguration(
    "org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory",
    connectionParams);

ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(transportConfiguration);

ClientSessionFactory sessionFactory = locator.createClientSessionFactory();

ClientSession session = sessionFactory.createSession(...);

etc
```

Similarly, if you're using JMS, you can configure the JMS connection
factory directly on the client side without having to define a connector
on the server side or define a connection factory in `activemq-jms.xml`:

``` java
Map<String, Object> connectionParams = new HashMap<String, Object>();

connectionParams.put(org.apache.activemq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, 5446);

TransportConfiguration transportConfiguration =
    new TransportConfiguration(
    "org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory",
    connectionParams);

ConnectionFactory connectionFactory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);

Connection jmsConnection = connectionFactory.createConnection();

etc
```

## Configuring the Netty transport

Out of the box, ActiveMQ currently uses
[Netty](http://netty.io/), a high performance low level
network library.

Our Netty transport can be configured in several different ways; to use
old (blocking) Java IO, or NIO (non-blocking), also to use
straightforward TCP sockets, SSL, or to tunnel over HTTP or HTTPS..

We believe this caters for the vast majority of transport requirements.

## Single Port Support

As of version 2.4 ActiveMQ now supports using a single port for all
protocols, ActiveMQ will automatically detect which protocol is being
used CORE, AMQP, STOMP or OPENWIRE and use the appropriate ActiveMQ
handler. It will also detect whether protocols such as HTTP or Web
Sockets are being used and also use the appropriate decoders

It is possible to limit which protocols are supported by using the
`protocols` parameter on the Acceptor like so:

        <param key="protocols" value="CORE,AMQP"/>
                

> **Note**
>
> The `protocol` parameter is now deprecated

## Configuring Netty TCP

Netty TCP is a simple unencrypted TCP sockets based transport. Netty TCP
can be configured to use old blocking Java IO or non blocking Java NIO.
We recommend you use the Java NIO on the server side for better
scalability with many concurrent connections. However using Java old IO
can sometimes give you better latency than NIO when you're not so
worried about supporting many thousands of concurrent connections.

If you're running connections across an untrusted network please bear in
mind this transport is unencrypted. You may want to look at the SSL or
HTTPS configurations.

With the Netty TCP transport all connections are initiated from the
client side. I.e. the server does not initiate any connections to the
client. This works well with firewall policies that typically only allow
connections to be initiated in one direction.

All the valid Netty transport keys are defined in the class
`org.apache.activemq.core.remoting.impl.netty.TransportConstants`. Most
parameters can be used either with acceptors or connectors, some only
work with acceptors. The following parameters can be used to configure
Netty for simple TCP:

-   `use-nio`. If this is `true` then Java non blocking NIO will be
    used. If set to `false` then old blocking Java IO will be used.

    If you require the server to handle many concurrent connections, we
    highly recommend that you use non blocking Java NIO. Java NIO does
    not maintain a thread per connection so can scale to many more
    concurrent connections than with old blocking IO. If you don't
    require the server to handle many concurrent connections, you might
    get slightly better performance by using old (blocking) IO. The
    default value for this property is `false` on the server side and
    `false` on the client side.

-   `host`. This specifies the host name or IP address to connect to
    (when configuring a connector) or to listen on (when configuring an
    acceptor). The default value for this property is `localhost`. When
    configuring acceptors, multiple hosts or IP addresses can be
    specified by separating them with commas. It is also possible to
    specify `0.0.0.0` to accept connection from all the host's network
    interfaces. It's not valid to specify multiple addresses when
    specifying the host for a connector; a connector makes a connection
    to one specific address.

    > **Note**
    >
    > Don't forget to specify a host name or IP address! If you want
    > your server able to accept connections from other nodes you must
    > specify a hostname or IP address at which the acceptor will bind
    > and listen for incoming connections. The default is localhost
    > which of course is not accessible from remote nodes!

-   `port`. This specified the port to connect to (when configuring a
    connector) or to listen on (when configuring an acceptor). The
    default value for this property is `5445`.

-   `tcp-no-delay`. If this is `true` then [Nagle's
    algorithm](http://en.wikipedia.org/wiki/Nagle%27s_algorithm) will be
    disabled. This is a [Java (client) socket
    option](http://docs.oracle.com/javase/7/docs/technotes/guides/net/socketOpt.html).
    The default value for this property is `true`.

-   `tcp-send-buffer-size`. This parameter determines the size of the
    TCP send buffer in bytes. The default value for this property is
    `32768` bytes (32KiB).

    TCP buffer sizes should be tuned according to the bandwidth and
    latency of your network. Here's a good link that explains the theory
    behind [this](http://www-didc.lbl.gov/TCP-tuning/).

    In summary TCP send/receive buffer sizes should be calculated as:

        buffer_size = bandwidth * RTT.

    Where bandwidth is in *bytes per second* and network round trip time
    (RTT) is in seconds. RTT can be easily measured using the `ping`
    utility.

    For fast networks you may want to increase the buffer sizes from the
    defaults.

-   `tcp-receive-buffer-size`. This parameter determines the size of the
    TCP receive buffer in bytes. The default value for this property is
    `32768` bytes (32KiB).

-   `batch-delay`. Before writing packets to the transport, ActiveMQ can
    be configured to batch up writes for a maximum of `batch-delay`
    milliseconds. This can increase overall throughput for very small
    messages. It does so at the expense of an increase in average
    latency for message transfer. The default value for this property is
    `0` ms.

-   `direct-deliver`. When a message arrives on the server and is
    delivered to waiting consumers, by default, the delivery is done on
    the same thread as that on which the message arrived. This gives
    good latency in environments with relatively small messages and a
    small number of consumers, but at the cost of overall throughput and
    scalability - especially on multi-core machines. If you want the
    lowest latency and a possible reduction in throughput then you can
    use the default value for `direct-deliver` (i.e. true). If you are
    willing to take some small extra hit on latency but want the highest
    throughput set `direct-deliver` to `false
                            `.

-   `nio-remoting-threads`. When configured to use NIO, ActiveMQ will,
    by default, use a number of threads equal to three times the number
    of cores (or hyper-threads) as reported by
    `Runtime.getRuntime().availableProcessors()` for processing incoming
    packets. If you want to override this value, you can set the number
    of threads by specifying this parameter. The default value for this
    parameter is `-1` which means use the value from
    `Runtime.getRuntime().availableProcessors()` \* 3.

-   `local-address`. When configured a Netty Connector it is possible to
    specify which local address the client will use when connecting to
    the remote address. This is typically used in the Application Server
    or when running Embedded to control which address is used for
    outbound connections. If the local-address is not set then the
    connector will use any local address available

-   `local-port`. When configured a Netty Connector it is possible to
    specify which local port the client will use when connecting to the
    remote address. This is typically used in the Application Server or
    when running Embedded to control which port is used for outbound
    connections. If the local-port default is used, which is 0, then the
    connector will let the system pick up an ephemeral port. valid ports
    are 0 to 65535

## Configuring Netty SSL

Netty SSL is similar to the Netty TCP transport but it provides
additional security by encrypting TCP connections using the Secure
Sockets Layer SSL

Please see the examples for a full working example of using Netty SSL.

Netty SSL uses all the same properties as Netty TCP but adds the
following additional properties:

-   `ssl-enabled`

    Must be `true` to enable SSL. Default is `false`.

-   `key-store-path`

    When used on an `acceptor` this is the path to the SSL key store on
    the server which holds the server's certificates (whether
    self-signed or signed by an authority).

    When used on a `connector` this is the path to the client-side SSL
    key store which holds the client certificates. This is only relevant
    for a `connector` if you are using 2-way SSL (i.e. mutual
    authentication). Although this value is configured on the server, it
    is downloaded and used by the client. If the client needs to use a
    different path from that set on the server then it can override the
    server-side setting by either using the customary
    "javax.net.ssl.keyStore" system property or the ActiveMQ-specific
    "org.apache.activemq.ssl.keyStore" system property. The
    ActiveMQ-specific system property is useful if another component on
    client is already making use of the standard, Java system property.

-   `key-store-password`

    When used on an `acceptor` this is the password for the server-side
    keystore.

    When used on a `connector` this is the password for the client-side
    keystore. This is only relevant for a `connector` if you are using
    2-way SSL (i.e. mutual authentication). Although this value can be
    configured on the server, it is downloaded and used by the client.
    If the client needs to use a different password from that set on the
    server then it can override the server-side setting by either using
    the customary "javax.net.ssl.keyStorePassword" system property or
    the ActiveMQ-specific "org.apache.activemq.ssl.keyStorePassword"
    system property. The ActiveMQ-specific system property is useful if
    another component on client is already making use of the standard,
    Java system property.

-   `trust-store-path`

    When used on an `acceptor` this is the path to the server-side SSL
    key store that holds the keys of all the clients that the server
    trusts. This is only relevant for an `acceptor` if you are using
    2-way SSL (i.e. mutual authentication).

    When used on a `connector` this is the path to the client-side SSL
    key store which holds the public keys of all the servers that the
    client trusts. Although this value can be configured on the server,
    it is downloaded and used by the client. If the client needs to use
    a different path from that set on the server then it can override
    the server-side setting by either using the customary
    "javax.net.ssl.trustStore" system property or the ActiveMQ-specific
    "org.apache.activemq.ssl.trustStore" system property. The
    ActiveMQ-specific system property is useful if another component on
    client is already making use of the standard, Java system property.

-   `trust-store-password`

    When used on an `acceptor` this is the password for the server-side
    trust store. This is only relevant for an `acceptor` if you are
    using 2-way SSL (i.e. mutual authentication).

    When used on a `connector` this is the password for the client-side
    truststore. Although this value can be configured on the server, it
    is downloaded and used by the client. If the client needs to use a
    different password from that set on the server then it can override
    the server-side setting by either using the customary
    "javax.net.ssl.trustStorePassword" system property or the
    ActiveMQ-specific "org.apache.activemq.ssl.trustStorePassword"
    system property. The ActiveMQ-specific system property is useful if
    another component on client is already making use of the standard,
    Java system property.

-   `enabled-cipher-suites`

    Whether used on an `acceptor` or `connector` this is a comma
    separated list of cipher suites used for SSL communication. The
    default value is `null` which means the JVM's default will be used.

-   `enabled-protocols`

    Whether used on an `acceptor` or `connector` this is a comma
    separated list of protocols used for SSL communication. The default
    value is `null` which means the JVM's default will be used.

-   `need-client-auth`

    This property is only for an `acceptor`. It tells a client
    connecting to this acceptor that 2-way SSL is required. Valid values
    are `true` or `false`. Default is `false`.

## Configuring Netty HTTP

Netty HTTP tunnels packets over the HTTP protocol. It can be useful in
scenarios where firewalls only allow HTTP traffic to pass.

Please see the examples for a full working example of using Netty HTTP.

Netty HTTP uses the same properties as Netty TCP but adds the following
additional properties:

-   `http-enabled`. This is now no longer needed as of version 2.4. With
    single port support ActiveMQ will now automatically detect if http
    is being used and configure itself.

-   `http-client-idle-time`. How long a client can be idle before
    sending an empty http request to keep the connection alive

-   `http-client-idle-scan-period`. How often, in milliseconds, to scan
    for idle clients

-   `http-response-time`. How long the server can wait before sending an
    empty http response to keep the connection alive

-   `http-server-scan-period`. How often, in milliseconds, to scan for
    clients needing responses

-   `http-requires-session-id`. If true the client will wait after the
    first call to receive a session id. Used the http connector is
    connecting to servlet acceptor (not recommended)
