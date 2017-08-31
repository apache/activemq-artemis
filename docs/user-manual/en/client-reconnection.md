# Client Reconnection and Session Reattachment

Apache ActiveMQ Artemis clients can be configured to automatically reconnect or
re-attach to the server in the event that a failure is detected in the
connection between the client and the server.

## 100% Transparent session re-attachment

If the failure was due to some transient failure such as a temporary
network failure, and the target server was not restarted, then the
sessions will still be existent on the server, assuming the client
hasn't been disconnected for more than connection-ttl [Detecting Dead Connections](connection-ttl.md)

In this scenario, Apache ActiveMQ Artemis will automatically re-attach the client
sessions to the server sessions when the connection reconnects. This is
done 100% transparently and the client can continue exactly as if
nothing had happened.

The way this works is as follows:

As Apache ActiveMQ Artemis clients send commands to their servers they store each sent
command in an in-memory buffer. In the case that connection failure
occurs and the client subsequently reattaches to the same server, as
part of the reattachment protocol the server informs the client during
reattachment with the id of the last command it successfully received
from that client.

If the client has sent more commands than were received before failover
it can replay any sent commands from its buffer so that the client and
server can reconcile their states.Ac

The size of this buffer is configured with the `confirmationWindowSize`
parameter on the connection URL. When the server has received
`confirmationWindowSize` bytes of commands and processed them it will
send back a command confirmation to the client, and the client can then
free up space in the buffer.

The window is specified in bytes.

Setting this parameter to `-1` disables any buffering and prevents any
re-attachment from occurring, forcing reconnect instead. The default
value for this parameter is `-1`. (Which means by default no auto
re-attachment will occur)

## Session reconnection

Alternatively, the server might have actually been restarted after
crashing or being stopped. In this case any sessions will no longer be
existent on the server and it won't be possible to 100% transparently
re-attach to them.

In this case, Apache ActiveMQ Artemis will automatically reconnect the connection and
*recreate* any sessions and consumers on the server corresponding to the
sessions and consumers on the client. This process is exactly the same
as what happens during failover onto a backup server.

Client reconnection is also used internally by components such as core
bridges to allow them to reconnect to their target servers.

Please see the section on failover [Automatic Client Failover](ha.md) to get a full understanding of how
transacted and non-transacted sessions are reconnected during
failover/reconnect and what you need to do to maintain *once and only
once*delivery guarantees.

## Configuring reconnection/reattachment attributes

Client reconnection is configured using the following parameters:

-   `retryInterval`. This optional parameter determines the period in
    milliseconds between subsequent reconnection attempts, if the
    connection to the target server has failed. The default value is
    `2000` milliseconds.

-   `retryIntervalMultiplier`. This optional parameter determines
    determines a multiplier to apply to the time since the last retry to
    compute the time to the next retry.

    This allows you to implement an *exponential backoff* between retry
    attempts.

    Let's take an example:

    If we set `retryInterval` to `1000` ms and we set
    `retryIntervalMultiplier` to `2.0`, then, if the first reconnect
    attempt fails, we will wait `1000` ms then `2000` ms then `4000` ms
    between subsequent reconnection attempts.

    The default value is `1.0` meaning each reconnect attempt is spaced
    at equal intervals.

-   `maxRetryInterval`. This optional parameter determines the maximum
    retry interval that will be used. When setting
    `retryIntervalMultiplier` it would otherwise be possible that
    subsequent retries exponentially increase to ridiculously large
    values. By setting this parameter you can set an upper limit on that
    value. The default value is `2000` milliseconds.

-   `reconnectAttempts`. This optional parameter determines the total
    number of reconnect attempts to make before giving up and shutting
    down. A value of `-1` signifies an unlimited number of attempts. The
    default value is `0`.

All of these parameters are set on the URL used to connect to the broker.

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
