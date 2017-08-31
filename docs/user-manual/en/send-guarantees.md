# Guarantees of sends and commits

## Guarantees of Transaction Completion

When committing or rolling back a transaction with Apache ActiveMQ Artemis, the request
to commit or rollback is sent to the server, and the call will block on
the client side until a response has been received from the server that
the commit or rollback was executed.

When the commit or rollback is received on the server, it will be
committed to the journal, and depending on the value of the parameter
`journal-sync-transactional` the server will ensure that the commit or
rollback is durably persisted to storage before sending the response
back to the client. If this parameter has the value `false` then commit
or rollback may not actually get persisted to storage until some time
after the response has been sent to the client. In event of server
failure this may mean the commit or rollback never gets persisted to
storage. The default value of this parameter is `true` so the client can
be sure all transaction commits or rollbacks have been persisted to
storage by the time the call to commit or rollback returns.

Setting this parameter to `false` can improve performance at the expense
of some loss of transaction durability.

This parameter is set in `broker.xml`

## Guarantees of Non Transactional Message Sends

If you are sending messages to a server using a non transacted session,
Apache ActiveMQ Artemis can be configured to block the call to send until the message
has definitely reached the server, and a response has been sent back to
the client. This can be configured individually for durable and
non-durable messages, and is determined by the following two URL parameters:

-   `blockOnDurableSend`. If this is set to `true` then all calls to
    send for durable messages on non transacted sessions will block
    until the message has reached the server, and a response has been
    sent back. The default value is `true`.

-   `blockOnNonDurableSend`. If this is set to `true` then all calls to
    send for non-durable messages on non transacted sessions will block
    until the message has reached the server, and a response has been
    sent back. The default value is `false`.

Setting block on sends to `true` can reduce performance since each send
requires a network round trip before the next send can be performed.
This means the performance of sending messages will be limited by the
network round trip time (RTT) of your network, rather than the bandwidth
of your network. For better performance we recommend either batching
many messages sends together in a transaction since with a transactional
session, only the commit / rollback blocks not every send, or, using
Apache ActiveMQ Artemis's advanced *asynchronous send acknowledgements feature*
described in Asynchronous Send Acknowledgements.

When the server receives a message sent from a non transactional
session, and that message is durable and the message is routed to at
least one durable queue, then the server will persist the message in
permanent storage. If the journal parameter
`journal-sync-non-transactional` is set to `true` the server will not
send a response back to the client until the message has been persisted
and the server has a guarantee that the data has been persisted to disk.
The default value for this parameter is `true`.

## Guarantees of Non Transactional Acknowledgements

If you are acknowledging the delivery of a message at the client side
using a non transacted session, Apache ActiveMQ Artemis can be configured to block the
call to acknowledge until the acknowledge has definitely reached the
server, and a response has been sent back to the client. This is
configured with the parameter `BlockOnAcknowledge`. If this is set to
`true` then all calls to acknowledge on non transacted sessions will
block until the acknowledge has reached the server, and a response has
been sent back. You might want to set this to `true` if you want to
implement a strict *at most once* delivery policy. The default value is
`false`

## Asynchronous Send Acknowledgements

If you are using a non transacted session but want a guarantee that
every message sent to the server has reached it, then, as discussed in
Guarantees of Non Transactional Message Sends, you can configure Apache ActiveMQ Artemis to block the call to send until the server
has received the message, persisted it and sent back a response. This
works well but has a severe performance penalty - each call to send
needs to block for at least the time of a network round trip (RTT) - the
performance of sending is thus limited by the latency of the network,
*not* limited by the network bandwidth.

Let's do a little bit of maths to see how severe that is. We'll consider
a standard 1Gib ethernet network with a network round trip between the
server and the client of 0.25 ms.

With a RTT of 0.25 ms, the client can send *at most* 1000/ 0.25 = 4000
messages per second if it blocks on each message send.

If each message is < 1500 bytes and a standard 1500 bytes MTU (Maximum Transmission Unit) size is
used on the network, then a 1GiB network has a *theoretical* upper limit
of (1024 \* 1024 \* 1024 / 8) / 1500 = 89478 messages per second if
messages are sent without blocking! These figures aren't an exact
science but you can clearly see that being limited by network RTT can
have serious effect on performance.

To remedy this, Apache ActiveMQ Artemis provides an advanced new feature called
*asynchronous send acknowledgements*. With this feature, Apache ActiveMQ Artemis can be
configured to send messages without blocking in one direction and
asynchronously getting acknowledgement from the server that the messages
were received in a separate stream. By de-coupling the send from the
acknowledgement of the send, the system is not limited by the network
RTT, but is limited by the network bandwidth. Consequently better
throughput can be achieved than is possible using a blocking approach,
while at the same time having absolute guarantees that messages have
successfully reached the server.

The window size for send acknowledgements is determined by the
confirmation-window-size parameter on the connection factory or client
session factory. Please see [Client Reconnection and Session Reattachment](client-reconnection.md) for more info on this.

# Asynchronous Send Acknowledgements

To use the feature using the core API, you implement the interface
`org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler` and set
a handler instance on your `ClientSession`.

Then, you just send messages as normal using your `ClientSession`, and
as messages reach the server, the server will send back an
acknowledgement of the send asynchronously, and some time later you are
informed at the client side by Apache ActiveMQ Artemis calling your handler's
`sendAcknowledged(ClientMessage message)` method, passing in a reference
to the message that was sent.

To enable asynchronous send acknowledgements you must make sure
`confirmationWindowSize` is set to a positive integer value, e.g.
10MiB

Please see [the examples chapter](examples.md) for a full working example.
