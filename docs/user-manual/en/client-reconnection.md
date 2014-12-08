Client Reconnection and Session Reattachment
============================================

ActiveMQ clients can be configured to automatically reconnect or
re-attach to the server in the event that a failure is detected in the
connection between the client and the server.

100% Transparent session re-attachment
======================================

If the failure was due to some transient failure such as a temporary
network failure, and the target server was not restarted, then the
sessions will still be existent on the server, assuming the client
hasn't been disconnected for more than connection-ttl ?.

In this scenario, ActiveMQ will automatically re-attach the client
sessions to the server sessions when the connection reconnects. This is
done 100% transparently and the client can continue exactly as if
nothing had happened.

The way this works is as follows:

As ActiveMQ clients send commands to their servers they store each sent
command in an in-memory buffer. In the case that connection failure
occurs and the client subsequently reattaches to the same server, as
part of the reattachment protocol the server informs the client during
reattachment with the id of the last command it successfully received
from that client.

If the client has sent more commands than were received before failover
it can replay any sent commands from its buffer so that the client and
server can reconcile their states.

The size of this buffer is configured by the `ConfirmationWindowSize`
parameter, when the server has received `ConfirmationWindowSize` bytes
of commands and processed them it will send back a command confirmation
to the client, and the client can then free up space in the buffer.

If you are using JMS and you're using the JMS service on the server to
load your JMS connection factory instances into JNDI then this parameter
can be configured in `activemq-jms.xml` using the element
`confirmation-window-size` a. If you're using JMS but not using JNDI
then you can set these values directly on the
`ActiveMQConnectionFactory` instance using the appropriate setter
method.

If you're using the core API you can set these values directly on the
`ServerLocator` instance using the appropriate setter method.

The window is specified in bytes.

Setting this parameter to `-1` disables any buffering and prevents any
re-attachment from occurring, forcing reconnect instead. The default
value for this parameter is `-1`. (Which means by default no auto
re-attachment will occur)

Session reconnection
====================

Alternatively, the server might have actually been restarted after
crashing or being stopped. In this case any sessions will no longer be
existent on the server and it won't be possible to 100% transparently
re-attach to them.

In this case, ActiveMQ will automatically reconnect the connection and
*recreate* any sessions and consumers on the server corresponding to the
sessions and consumers on the client. This process is exactly the same
as what happens during failover onto a backup server.

Client reconnection is also used internally by components such as core
bridges to allow them to reconnect to their target servers.

Please see the section on failover ? to get a full understanding of how
transacted and non-transacted sessions are reconnected during
failover/reconnect and what you need to do to maintain *once and only
once*delivery guarantees.

Configuring reconnection/reattachment attributes
================================================

Client reconnection is configured using the following parameters:

-   `retry-interval`. This optional parameter determines the period in
    milliseconds between subsequent reconnection attempts, if the
    connection to the target server has failed. The default value is
    `2000` milliseconds.

-   `retry-interval-multiplier`. This optional parameter determines
    determines a multiplier to apply to the time since the last retry to
    compute the time to the next retry.

    This allows you to implement an *exponential backoff* between retry
    attempts.

    Let's take an example:

    If we set `retry-interval` to `1000` ms and we set
    `retry-interval-multiplier` to `2.0`, then, if the first reconnect
    attempt fails, we will wait `1000` ms then `2000` ms then `4000` ms
    between subsequent reconnection attempts.

    The default value is `1.0` meaning each reconnect attempt is spaced
    at equal intervals.

-   `max-retry-interval`. This optional parameter determines the maximum
    retry interval that will be used. When setting
    `retry-interval-multiplier` it would otherwise be possible that
    subsequent retries exponentially increase to ridiculously large
    values. By setting this parameter you can set an upper limit on that
    value. The default value is `2000` milliseconds.

-   `reconnect-attempts`. This optional parameter determines the total
    number of reconnect attempts to make before giving up and shutting
    down. A value of `-1` signifies an unlimited number of attempts. The
    default value is `0`.

If you're using JMS and you're using JNDI on the client to look up your
JMS connection factory instances then you can specify these parameters
in the JNDI context environment in, e.g. `jndi.properties`:

    java.naming.factory.initial = org.apache.activemq.jndi.ActiveMQInitialContextFactory
    java.naming.provider.url = tcp://localhost:5445
    connection.ConnectionFactory.retryInterval=1000
    connection.ConnectionFactory.retryIntervalMultiplier=1.5
    connection.ConnectionFactory.maxRetryInterval=60000
    connection.ConnectionFactory.reconnectAttempts=1000

If you're using JMS, but instantiating your JMS connection factory
directly, you can specify the parameters using the appropriate setter
methods on the `ActiveMQConnectionFactory` immediately after creating
it.

If you're using the core API and instantiating the `ServerLocator`
instance directly you can also specify the parameters using the
appropriate setter methods on the `ServerLocator` immediately after
creating it.

If your client does manage to reconnect but the session is no longer
available on the server, for instance if the server has been restarted
or it has timed out, then the client won't be able to re-attach, and any
`ExceptionListener` or `FailureListener` instances registered on the
connection or session will be called.

ExceptionListeners and SessionFailureListeners
==============================================

Please note, that when a client reconnects or re-attaches, any
registered JMS `ExceptionListener` or core API `SessionFailureListener`
will be called.
