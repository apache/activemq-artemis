# Performance Tuning

In this chapter we'll discuss how to tune ActiveMQ for optimum
performance.

## Tuning persistence

-   Put the message journal on its own physical volume. If the disk is
    shared with other processes e.g. transaction co-ordinator, database
    or other journals which are also reading and writing from it, then
    this may greatly reduce performance since the disk head may be
    skipping all over the place between the different files. One of the
    advantages of an append only journal is that disk head movement is
    minimised - this advantage is destroyed if the disk is shared. If
    you're using paging or large messages make sure they're ideally put
    on separate volumes too.

-   Minimum number of journal files. Set `journal-min-files` to a number
    of files that would fit your average sustainable rate. If you see
    new files being created on the journal data directory too often,
    i.e. lots of data is being persisted, you need to increase the
    minimal number of files, this way the journal would reuse more files
    instead of creating new data files.

-   Journal file size. The journal file size should be aligned to the
    capacity of a cylinder on the disk. The default value 10MiB should
    be enough on most systems.

-   Use AIO journal. If using Linux, try to keep your journal type as
    AIO. AIO will scale better than Java NIO.

-   Tune `journal-buffer-timeout`. The timeout can be increased to
    increase throughput at the expense of latency.

-   If you're running AIO you might be able to get some better
    performance by increasing `journal-max-io`. DO NOT change this
    parameter if you are running NIO.

## Tuning JMS

There are a few areas where some tweaks can be done if you are using the
JMS API

-   Disable message id. Use the `setDisableMessageID()` method on the
    `MessageProducer` class to disable message ids if you don't need
    them. This decreases the size of the message and also avoids the
    overhead of creating a unique ID.

-   Disable message timestamp. Use the `setDisableMessageTimeStamp()`
    method on the `MessageProducer` class to disable message timestamps
    if you don't need them.

-   Avoid `ObjectMessage`. `ObjectMessage` is convenient but it comes at
    a cost. The body of a `ObjectMessage` uses Java serialization to
    serialize it to bytes. The Java serialized form of even small
    objects is very verbose so takes up a lot of space on the wire, also
    Java serialization is slow compared to custom marshalling
    techniques. Only use `ObjectMessage` if you really can't use one of
    the other message types, i.e. if you really don't know the type of
    the payload until run-time.

-   Avoid `AUTO_ACKNOWLEDGE`. `AUTO_ACKNOWLEDGE` mode requires an
    acknowledgement to be sent from the server for each message received
    on the client, this means more traffic on the network. If you can,
    use `DUPS_OK_ACKNOWLEDGE` or use `CLIENT_ACKNOWLEDGE` or a
    transacted session and batch up many acknowledgements with one
    acknowledge/commit.

-   Avoid durable messages. By default JMS messages are durable. If you
    don't really need durable messages then set them to be non-durable.
    Durable messages incur a lot more overhead in persisting them to
    storage.

-   Batch many sends or acknowledgements in a single transaction.
    ActiveMQ will only require a network round trip on the commit, not
    on every send or acknowledgement.

## Other Tunings

There are various other places in ActiveMQ where we can perform some
tuning:

-   Use Asynchronous Send Acknowledgements. If you need to send durable
    messages non transactionally and you need a guarantee that they have
    reached the server by the time the call to send() returns, don't set
    durable messages to be sent blocking, instead use asynchronous send
    acknowledgements to get your acknowledgements of send back in a
    separate stream, see [Guarantees of sends and commits](send-guarantees.md) 
    for more information on this.

-   Use pre-acknowledge mode. With pre-acknowledge mode, messages are
    acknowledged `before` they are sent to the client. This reduces the
    amount of acknowledgement traffic on the wire. For more information
    on this, see [Extra Acknowledge Modes](pre-acknowledge.md).

-   Disable security. You may get a small performance boost by disabling
    security by setting the `security-enabled` parameter to `false` in
    `activemq-configuration.xml`.

-   Disable persistence. If you don't need message persistence, turn it
    off altogether by setting `persistence-enabled` to false in
    `activemq-configuration.xml`.

-   Sync transactions lazily. Setting `journal-sync-transactional` to
    `false` in `activemq-configuration.xml` can give you better
    transactional persistent performance at the expense of some
    possibility of loss of transactions on failure. See  [Guarantees of sends and commits](send-guarantees.md) 
    for more information.

-   Sync non transactional lazily. Setting
    `journal-sync-non-transactional` to `false` in
    `activemq-configuration.xml` can give you better non-transactional
    persistent performance at the expense of some possibility of loss of
    durable messages on failure. See  [Guarantees of sends and commits](send-guarantees.md)
    for more information.

-   Send messages non blocking. Setting `block-on-durable-send` and
    `block-on-non-durable-send` to `false` in `activemq-jms.xml` (if
    you're using JMS and JNDI) or directly on the ServerLocator. This
    means you don't have to wait a whole network round trip for every
    message sent. See  [Guarantees of sends and commits](send-guarantees.md) 
    for more information.

-   If you have very fast consumers, you can increase
    consumer-window-size. This effectively disables consumer flow
    control.

-   Socket NIO vs Socket Old IO. By default ActiveMQ uses old (blocking)
    on the server and the client side (see the chapter on configuring
    transports for more information [Configuring the Transport](configuring-transports.md). NIO is much more scalable but
    can give you some latency hit compared to old blocking IO. If you
    need to be able to service many thousands of connections on the
    server, then you should make sure you're using NIO on the server.
    However, if don't expect many thousands of connections on the server
    you can keep the server acceptors using old IO, and might get a
    small performance advantage.

-   Use the core API not JMS. Using the JMS API you will have slightly
    lower performance than using the core API, since all JMS operations
    need to be translated into core operations before the server can
    handle them. If using the core API try to use methods that take
    `SimpleString` as much as possible. `SimpleString`, unlike
    java.lang.String does not require copying before it is written to
    the wire, so if you re-use `SimpleString` instances between calls
    then you can avoid some unnecessary copying.

## Tuning Transport Settings

-   TCP buffer sizes. If you have a fast network and fast machines you
    may get a performance boost by increasing the TCP send and receive
    buffer sizes. See the [Configuring the Transport](configuring-transports.md) 
    for more information on this.

    > **Note**
    >
    > Note that some operating systems like later versions of Linux
    > include TCP auto-tuning and setting TCP buffer sizes manually can
    > prevent auto-tune from working and actually give you worse
    > performance!

-   Increase limit on file handles on the server. If you expect a lot of
    concurrent connections on your servers, or if clients are rapidly
    opening and closing connections, you should make sure the user
    running the server has permission to create sufficient file handles.

    This varies from operating system to operating system. On Linux
    systems you can increase the number of allowable open file handles
    in the file `/etc/security/limits.conf` e.g. add the lines

        serveruser     soft    nofile  20000
        serveruser     hard    nofile  20000

    This would allow up to 20000 file handles to be open by the user
    `serveruser`.

-   Use `batch-delay` and set `direct-deliver` to false for the best
    throughput for very small messages. ActiveMQ comes with a
    preconfigured connector/acceptor pair (`netty-throughput`) in
    `activemq-configuration.xml` and JMS connection factory
    (`ThroughputConnectionFactory`) in `activemq-jms.xml`which can be
    used to give the very best throughput, especially for small
    messages. See the [Configuring the Transport](configuring-transports.md) 
    for more information on this.

## Tuning the VM

We highly recommend you use the latest Java JVM for the best
performance. We test internally using the Sun JVM, so some of these
tunings won't apply to JDKs from other providers (e.g. IBM or JRockit)

-   Garbage collection. For smooth server operation we recommend using a
    parallel garbage collection algorithm, e.g. using the JVM argument
    `-XX:+UseParallelOldGC` on Sun JDKs.

-   Memory settings. Give as much memory as you can to the server.
    ActiveMQ can run in low memory by using paging (described in [Paging](paging.md)) but
    if it can run with all queues in RAM this will improve performance.
    The amount of memory you require will depend on the size and number
    of your queues and the size and number of your messages. Use the JVM
    arguments `-Xms` and `-Xmx` to set server available RAM. We
    recommend setting them to the same high value.

-   Aggressive options. Different JVMs provide different sets of JVM
    tuning parameters, for the Sun Hotspot JVM the full list of options
    is available
    [here](http://www.oracle.com/technetwork/java/javase/tech/vmoptions-jsp-140102.html).
    We recommend at least using `-XX:+AggressiveOpts` and`
                            -XX:+UseFastAccessorMethods`. You may get
    some mileage with the other tuning parameters depending on your OS
    platform and application usage patterns.

## Avoiding Anti-Patterns

-   Re-use connections / sessions / consumers / producers. Probably the
    most common messaging anti-pattern we see is users who create a new
    connection/session/producer for every message they send or every
    message they consume. This is a poor use of resources. These objects
    take time to create and may involve several network round trips.
    Always re-use them.

    > **Note**
    >
    > Some popular libraries such as the Spring JMS Template are known
    > to use these anti-patterns. If you're using Spring JMS Template
    > and you're getting poor performance you know why. Don't blame
    > ActiveMQ! The Spring JMS Template can only safely be used in an
    > app server which caches JMS sessions (e.g. using JCA), and only
    > then for sending messages. It cannot be safely be used for
    > synchronously consuming messages, even in an app server.

-   Avoid fat messages. Verbose formats such as XML take up a lot of
    space on the wire and performance will suffer as result. Avoid XML
    in message bodies if you can.

-   Don't create temporary queues for each request. This common
    anti-pattern involves the temporary queue request-response pattern.
    With the temporary queue request-response pattern a message is sent
    to a target and a reply-to header is set with the address of a local
    temporary queue. When the recipient receives the message they
    process it then send back a response to the address specified in the
    reply-to. A common mistake made with this pattern is to create a new
    temporary queue on each message sent. This will drastically reduce
    performance. Instead the temporary queue should be re-used for many
    requests.

-   Don't use Message-Driven Beans for the sake of it. As soon as you
    start using MDBs you are greatly increasing the codepath for each
    message received compared to a straightforward message consumer,
    since a lot of extra application server code is executed. Ask
    yourself do you really need MDBs? Can you accomplish the same task
    using just a normal message consumer?


