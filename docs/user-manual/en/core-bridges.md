# Core Bridges

The function of a bridge is to consume messages from a source queue, and
forward them to a target address, typically on a different Apache ActiveMQ Artemis
server.

The source and target servers do not have to be in the same cluster
which makes bridging suitable for reliably sending messages from one
cluster to another, for instance across a WAN, or internet and where the
connection may be unreliable.

The bridge has built in resilience to failure so if the target server
connection is lost, e.g. due to network failure, the bridge will retry
connecting to the target until it comes back online. When it comes back
online it will resume operation as normal.

In summary, bridges are a way to reliably connect two separate Apache ActiveMQ Artemis
servers together. With a core bridge both source and target servers must
be Apache ActiveMQ Artemis servers.

Bridges can be configured to provide *once and only once* delivery
guarantees even in the event of the failure of the source or the target
server. They do this by using duplicate detection (described in [Duplicate Detection](duplicate-detection.md)).

> **Note**
>
> Although they have similar function, don't confuse core bridges with
> JMS bridges!
>
> Core bridges are for linking an Apache ActiveMQ Artemis node with another Apache ActiveMQ Artemis
> node and do not use the JMS API. A JMS Bridge is used for linking any
> two JMS 1.1 compliant JMS providers. So, a JMS Bridge could be used
> for bridging to or from different JMS compliant messaging system. It's
> always preferable to use a core bridge if you can. Core bridges use
> duplicate detection to provide *once and only once* guarantees. To
> provide the same guarantee using a JMS bridge you would have to use XA
> which has a higher overhead and is more complex to configure.

## Configuring Bridges

Bridges are configured in `broker.xml`. Let's kick off
with an example (this is actually from the bridge example):

```xml
<bridge name="my-bridge">
   <queue-name>sausage-factory</queue-name>
   <forwarding-address>mincing-machine</forwarding-address>
   <filter string="name='aardvark'"/>
   <transformer-class-name>
      org.apache.activemq.artemis.jms.example.HatColourChangeTransformer
   </transformer-class-name>
   <retry-interval>1000</retry-interval>
   <ha>true</ha>
   <retry-interval-multiplier>1.0</retry-interval-multiplier>
   <initial-connect-attempts>-1</initial-connect-attempts>
   <reconnect-attempts>-1</reconnect-attempts>
   <failover-on-server-shutdown>false</failover-on-server-shutdown>
   <use-duplicate-detection>true</use-duplicate-detection>
   <confirmation-window-size>10000000</confirmation-window-size>
   <user>foouser</user>
   <password>foopassword</password>
   <static-connectors>
      <connector-ref>remote-connector</connector-ref>
   </static-connectors>
   <!-- alternative to static-connectors
   <discovery-group-ref discovery-group-name="bridge-discovery-group"/>
   -->
</bridge>
```

In the above example we have shown all the parameters its possible to
configure for a bridge. In practice you might use many of the defaults
so it won't be necessary to specify them all explicitly.

Let's take a look at all the parameters in turn:

-   `name` attribute. All bridges must have a unique name in the server.

-   `queue-name`. This is the unique name of the local queue that the
    bridge consumes from, it's a mandatory parameter.

    The queue must already exist by the time the bridge is instantiated
    at start-up.

-   `forwarding-address`. This is the address on the target server that
    the message will be forwarded to. If a forwarding address is not
    specified, then the original address of the message will be
    retained.

-   `filter-string`. An optional filter string can be supplied. If
    specified then only messages which match the filter expression
    specified in the filter string will be forwarded. The filter string
    follows the ActiveMQ Artemis filter expression syntax described in [Filter Expressions](filter-expressions.md).

-   `transformer-class-name`. An optional transformer-class-name can be
    specified. This is the name of a user-defined class which implements
    the `org.apache.activemq.artemis.core.server.transformer.Transformer` interface.

    If this is specified then the transformer's `transform()` method
    will be invoked with the message before it is forwarded. This gives
    you the opportunity to transform the message's header or body before
    forwarding it.

-   `ha`. This optional parameter determines whether or not this bridge
    should support high availability. True means it will connect to any
    available server in a cluster and support failover. The default
    value is `false`.

-   `retry-interval`. This optional parameter determines the period in
    milliseconds between subsequent reconnection attempts, if the
    connection to the target server has failed. The default value is
    `2000`milliseconds.

-   `retry-interval-multiplier`. This optional parameter determines
    determines a multiplier to apply to the time since the last retry to
    compute the time to the next retry.

    This allows you to implement an *exponential backoff* between retry
    attempts.

    Let's take an example:

    If we set `retry-interval`to `1000` ms and we set
    `retry-interval-multiplier` to `2.0`, then, if the first reconnect
    attempt fails, we will wait `1000` ms then `2000` ms then `4000` ms
    between subsequent reconnection attempts.

    The default value is `1.0` meaning each reconnect attempt is spaced
    at equal intervals.

-   `initial-connect-attempts`. This optional parameter determines the
    total number of initial connect attempts the bridge will make before
    giving up and shutting down. A value of `-1` signifies an unlimited
    number of attempts. The default value is `-1`.

-   `reconnect-attempts`. This optional parameter determines the total
    number of reconnect attempts the bridge will make before giving up
    and shutting down. A value of `-1` signifies an unlimited number of
    attempts. The default value is `-1`.

-   `failover-on-server-shutdown`. This optional parameter determines
    whether the bridge will attempt to failover onto a backup server (if
    specified) when the target server is cleanly shutdown rather than
    crashed.

    The bridge connector can specify both a live and a backup server, if
    it specifies a backup server and this parameter is set to `true`
    then if the target server is *cleanly* shutdown the bridge
    connection will attempt to failover onto its backup. If the bridge
    connector has no backup server configured then this parameter has no
    effect.

    Sometimes you want a bridge configured with a live and a backup
    target server, but you don't want to failover to the backup if the
    live server is simply taken down temporarily for maintenance, this
    is when this parameter comes in handy.

    The default value for this parameter is `false`.

-   `use-duplicate-detection`. This optional parameter determines
    whether the bridge will automatically insert a duplicate id property
    into each message that it forwards.

    Doing so, allows the target server to perform duplicate detection on
    messages it receives from the source server. If the connection fails
    or server crashes, then, when the bridge resumes it will resend
    unacknowledged messages. This might result in duplicate messages
    being sent to the target server. By enabling duplicate detection
    allows these duplicates to be screened out and ignored.

    This allows the bridge to provide a *once and only once* delivery
    guarantee without using heavyweight methods such as XA (see [Duplicate Detection](duplicate-detection.md) for
    more information).

    The default value for this parameter is `true`.

-   `confirmation-window-size`. This optional parameter determines the
    `confirmation-window-size` to use for the connection used to forward
    messages to the target node. This attribute is described in section
    [Reconnection and Session Reattachment](client-reconnection.md)

    > **Warning**
    >
    > When using the bridge to forward messages to an address which uses
    > the `BLOCK` `address-full-policy` from a queue which has a
    > `max-size-bytes` set it's important that
    > `confirmation-window-size` is less than or equal to
    > `max-size-bytes` to prevent the flow of messages from ceasing.

-   `producer-window-size`. This optional parameter determines the
    producer flow control through the bridge. You usually leave this off
    unless you are dealing with huge large messages. 
    
    Default=-1 (disabled)

-   `user`. This optional parameter determines the user name to use when
    creating the bridge connection to the remote server. If it is not
    specified the default cluster user specified by `cluster-user` in
    `broker.xml` will be used.

-   `password`. This optional parameter determines the password to use
    when creating the bridge connection to the remote server. If it is
    not specified the default cluster password specified by
    `cluster-password` in `broker.xml` will be used.

-   `static-connectors` or `discovery-group-ref`. Pick either of these
    options to connect the bridge to the target server.

    The `static-connectors` is a list of `connector-ref` elements
    pointing to `connector` elements defined elsewhere. A *connector*
    encapsulates knowledge of what transport to use (TCP, SSL, HTTP etc)
    as well as the server connection parameters (host, port etc). For
    more information about what connectors are and how to configure
    them, please see [Configuring the Transport](configuring-transports.md).

    The `discovery-group-ref` element has one attribute -
    `discovery-group-name`. This attribute points to a `discovery-group`
    defined elsewhere. For more information about what discovery-groups
    are and how to configure them, please see [Discovery Groups](clusters.md).


