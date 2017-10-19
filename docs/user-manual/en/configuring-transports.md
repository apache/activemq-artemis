# Configuring the Transport

In this chapter we'll describe the concepts required for understanding
Apache ActiveMQ Artemis transports and where and how they're configured.

## Understanding Acceptors

One of the most important concepts in Apache ActiveMQ Artemis transports is the
*acceptor*. Let's dive straight in and take a look at an acceptor
defined in xml in the configuration file `broker.xml`.

    <acceptors>
       <acceptor name="netty">tcp://localhost:61617</acceptor>
    </acceptors>

Acceptors are always defined inside an `acceptors` element. There can be
one or more acceptors defined in the `acceptors` element. There's no
upper limit to the number of acceptors per server.

Each acceptor defines a way in which connections can be made to the
Apache ActiveMQ Artemis server.

In the above example we're defining an acceptor that uses
[Netty](http://netty.io/) to listen for connections at port
`61617`.

The `acceptor` element contains a `URL` that defines the kind of Acceptor
to create along with its configuration. The `schema` part of the `URL`
defines the Acceptor type which can either be `tcp` or `vm` which is
`Netty` or an In VM Acceptor respectively. For `Netty` the host and the
port of the `URL` define what host and port the `acceptor` will bind to. For
In VM the `Authority` part of the `URL` defines a unique server id.

The `acceptor` can also be configured with a set of key=value pairs
used to configure the specific transport, the set of
valid key=value pairs depends on the specific transport be used and are
passed straight through to the underlying transport. These are set on the
`URL` as part of the query, like so:

    <acceptor name="netty">tcp://localhost:61617?sslEnabled=true&keyStorePath=/path</acceptor>

## Understanding Connectors

Whereas acceptors are used on the server to define how we accept
connections, connectors are used to define how to connect to a server.

Let's look at a connector defined in our `broker.xml` file:

    <connectors>
       <connector name="netty">tcp://localhost:61617</connector>
    </connectors>

Connectors can be defined inside a `connectors` element. There can be
one or more connectors defined in the `connectors` element. There's no
upper limit to the number of connectors per server.

A `connector` is used when the server acts as a client itself, e.g.:

-   When one server is bridged to another
-   When a server takes part in a cluster

In these cases the server needs to know how to connect to other servers.
That's defined by `connectors`.

## Configuring the transport directly from the client side.

How do we configure a core `ClientSessionFactory` with the information
that it needs to connect with a server?

Connectors are also used indirectly when configuring a core
`ClientSessionFactory` to directly talk to a server. Although in this
case there's no need to define such a connector in the server side
configuration, instead we just specify the appropriate URI.

Here's an example of creating a `ClientSessionFactory` which will
connect directly to the acceptor we defined earlier in this chapter, it
uses the standard Netty TCP transport and will try and connect on port
61617 to localhost (default):

``` java
ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61617");

ClientSessionFactory sessionFactory = locator.createClientSessionFactory();

ClientSession session = sessionFactory.createSession(...);
```

Similarly, if you're using JMS, you can configure the JMS connection
factory directly on the client side:

``` java
ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61617");

Connection jmsConnection = connectionFactory.createConnection();
```

## Configuring the Netty transport

Out of the box, Apache ActiveMQ Artemis currently uses
[Netty](http://netty.io/), a high performance low level
network library.

Our Netty transport can be configured in several different ways; to use
straightforward TCP sockets, SSL, or to tunnel over HTTP or HTTPS..

We believe this caters for the vast majority of transport requirements.

## Single Port Support

Apache ActiveMQ Artemis supports using a single port for all
protocols, Apache ActiveMQ Artemis will automatically detect which protocol is being
used CORE, AMQP, STOMP or OPENWIRE and use the appropriate Apache ActiveMQ Artemis
handler. It will also detect whether protocols such as HTTP or Web
Sockets are being used and also use the appropriate decoders

It is possible to limit which protocols are supported by using the
`protocols` parameter on the Acceptor like so:

        <connector name="netty">tcp://localhost:61617?protocols=CORE,AMQP</connector>

## Configuring Netty TCP

Netty TCP is a simple unencrypted TCP sockets based transport. If you're
running connections across an untrusted network please bear in
mind this transport is unencrypted. You may want to look at the SSL or
HTTPS configurations.

With the Netty TCP transport all connections are initiated from the
client side (i.e. the server does not initiate any connections to the
client). This works well with firewall policies that typically only allow
connections to be initiated in one direction.

All the valid keys for the `tcp` URL scheme used for Netty are defined in the
class `org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants`.
Most parameters can be used either with acceptors or connectors, some only
work with acceptors. The following parameters can be used to configure
Netty for simple TCP:

> **Note**
>
> The `host` and `port` parameters are only used in the core API, in
> XML configuration these are set in the URI host and port.

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
    default value for this property is `61616`.

-   `tcpNoDelay`. If this is `true` then [Nagle's
    algorithm](http://en.wikipedia.org/wiki/Nagle%27s_algorithm) will be
    disabled. This is a [Java (client) socket
    option](http://docs.oracle.com/javase/7/docs/technotes/guides/net/socketOpt.html).
    The default value for this property is `true`.

-   `tcpSendBufferSize`. This parameter determines the size of the
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

-   `tcpReceiveBufferSize`. This parameter determines the size of the
    TCP receive buffer in bytes. The default value for this property is
    `32768` bytes (32KiB).
    
-   `writeBufferLowWaterMark`. This parameter determines the low water mark of 
    the Netty write buffer. Once the number of bytes queued in the write buffer exceeded 
    the high water mark and then dropped down below this value, Netty's channel 
    will start to be writable again. The default value for this property is 
    `32768` bytes (32KiB).
 
-   `writeBufferHighWaterMark`. This parameter determines the high water mark of 
    the Netty write buffer. If the number of bytes queued in the write buffer exceeds 
    this value, Netty's channel will start to be not writable. The default value for 
    this property is `131072` bytes (128KiB).

-   `batchDelay`. Before writing packets to the transport, Apache ActiveMQ Artemis can
    be configured to batch up writes for a maximum of `batchDelay`
    milliseconds. This can increase overall throughput for very small
    messages. It does so at the expense of an increase in average
    latency for message transfer. The default value for this property is
    `0` ms.

-   `directDeliver`. When a message arrives on the server and is
    delivered to waiting consumers, by default, the delivery is done on
    the same thread as that on which the message arrived. This gives
    good latency in environments with relatively small messages and a
    small number of consumers, but at the cost of overall throughput and
    scalability - especially on multi-core machines. If you want the
    lowest latency and a possible reduction in throughput then you can
    use the default value for `directDeliver` (i.e. `true`). If you are
    willing to take some small extra hit on latency but want the highest
    throughput set `directDeliver` to `false`.

-   `nioRemotingThreads` This is deprecated. It is replaced by `remotingThreads`, 
    if you are using this please update your configuration

-   `remotingThreads`. Apache ActiveMQ Artemis will,
    by default, use a number of threads equal to three times the number
    of cores (or hyper-threads) as reported by
    `Runtime.getRuntime().availableProcessors()` for processing incoming
    packets. If you want to override this value, you can set the number
    of threads by specifying this parameter. The default value for this
    parameter is `-1` which means use the value from
    `Runtime.getRuntime().availableProcessors()` \* 3.

-   `localAddress`. When configured a Netty Connector it is possible to
    specify which local address the client will use when connecting to
    the remote address. This is typically used in the Application Server
    or when running Embedded to control which address is used for
    outbound connections. If the local-address is not set then the
    connector will use any local address available

-   `localPort`. When configured a Netty Connector it is possible to
    specify which local port the client will use when connecting to the
    remote address. This is typically used in the Application Server or
    when running Embedded to control which port is used for outbound
    connections. If the local-port default is used, which is 0, then the
    connector will let the system pick up an ephemeral port. valid ports
    are 0 to 65535

-   `connectionsAllowed`. This is only valid for acceptors. It limits the
    number of connections which the acceptor will allow. When this limit
    is reached a DEBUG level message is issued to the log, and the connection
    is refused. The type of client in use will determine what happens when
    the connection is refused. In the case of a `core` client, it will
    result in a `org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException`.

## Configuring Netty Native Transport

Netty Native Transport support exists for selected OS platforms.
This allows Apache ActiveMQ Artemis to use native sockets/io instead of Java NIO.

These Native transports add features specific to a particular platform, 
generate less garbage, and generally improve performance when compared to Java NIO based transport.

Both Clients and Server can benefit from this.
 
Current Supported Platforms.
-    Linux running 64bit JVM
-    MacOS running 64bit JVM

Apache ActiveMQ Artemis will by default enable the corresponding native transport if a supported platform is detected.

If running on an unsupported platform or any issues loading native libs, Apache ActiveMQ Artemis will fallback onto Java NIO.

#### Linux Native Transport

On supported Linux platforms Epoll is used, @see https://en.wikipedia.org/wiki/Epoll. 

The following properties are specific to this native transport:

-  `useEpoll` enables the use of epoll if a supported linux platform is running a 64bit JVM is detected. 
    Setting this to `false` will force the use of Java NIO instead of epoll. Default is `true`

#### MacOS Native Transport

On supported MacOS platforms KQueue is used, @see https://en.wikipedia.org/wiki/Kqueue. 

The following properties are specific to this native transport:

-  `useKQueue` enables the use of kqueue if a supported MacOS platform running a 64bit JVM is detected. 
    Setting this to `false` will force the use of Java NIO instead of kqueue. Default is `true`

    
## Configuring Netty SSL

Netty SSL is similar to the Netty TCP transport but it provides
additional security by encrypting TCP connections using the Secure
Sockets Layer SSL

Please see the examples for a full working example of using Netty SSL.

Netty SSL uses all the same properties as Netty TCP but adds the
following additional properties:

-   `sslEnabled`

    Must be `true` to enable SSL. Default is `false`.

-   `keyStorePath`

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

-   `keyStorePassword`

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

-   `trustStorePath`

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

-   `trustStorePassword`

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

-   `enabledCipherSuites`

    Whether used on an `acceptor` or `connector` this is a comma
    separated list of cipher suites used for SSL communication. The
    default value is `null` which means the JVM's default will be used.

-   `enabledProtocols`

    Whether used on an `acceptor` or `connector` this is a comma
    separated list of protocols used for SSL communication. The default
    value is `null` which means the JVM's default will be used.

-   `needClientAuth`

    This property is only for an `acceptor`. It tells a client
    connecting to this acceptor that 2-way SSL is required. Valid values
    are `true` or `false`. Default is `false`.

-   `verifyHost`

    When used on an `acceptor` the `CN` of the connecting client's SSL certificate
    will be compared to its hostname to verify they match. This is useful
    only for 2-way SSL.

    When used on a `connector` the `CN` of the server's SSL certificate will be
    compared to its hostname to verify they match. This is useful for both 1-way
    and 2-way SSL.

    Valid values are `true` or `false`. Default is `false`.
    
-   `trustAll`

    When used on a `connector` the client will trust the provided server certificate
    implicitly, regardless of any configured trust store.  **Warning:** This setting is
    primarily for testing purposes only and should not be used in production.

    Valid values are `true` or `false`. Default is `false`.    

-   `useDefaultSslContext`

    Only valid on a `connector`. Allows the `connector` to use the "default" SSL
    context (via `SSLContext.getDefault()`) which can be set programmatically by
    the client (via `SSLContext.setDefault(SSLContext)`). If set to `true` all
    other SSL related parameters except for `sslEnabled` are ignored.

    Valid values are `true` or `false`. Default is `false`.

## Configuring Netty HTTP

Netty HTTP tunnels packets over the HTTP protocol. It can be useful in
scenarios where firewalls only allow HTTP traffic to pass.

Please see the examples for a full working example of using Netty HTTP.

Netty HTTP uses the same properties as Netty TCP but adds the following
additional properties:

-   `httpEnabled`. This is now no longer needed. With single port support
    Apache ActiveMQ Artemis will now automatically detect if http is being
    used and configure itself.

-   `httpClientIdleTime`. How long a client can be idle before
    sending an empty http request to keep the connection alive

-   `httpClientIdleScanPeriod`. How often, in milliseconds, to scan
    for idle clients

-   `httpResponseTime`. How long the server can wait before sending an
    empty http response to keep the connection alive

-   `httpServerScanPeriod`. How often, in milliseconds, to scan for
    clients needing responses

-   `httpRequiresSessionId`. If `true` the client will wait after the
    first call to receive a session id. Used the http connector is
    connecting to servlet acceptor (not recommended)
