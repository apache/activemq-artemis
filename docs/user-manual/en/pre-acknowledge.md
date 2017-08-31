# Extra Acknowledge Modes

JMS specifies 3 acknowledgement modes:

-   `AUTO_ACKNOWLEDGE`

-   `CLIENT_ACKNOWLEDGE`

-   `DUPS_OK_ACKNOWLEDGE`

Apache ActiveMQ Artemis supports two additional modes: `PRE_ACKNOWLEDGE` and
`INDIVIDUAL_ACKNOWLEDGE`

In some cases you can afford to lose messages in event of failure, so it
would make sense to acknowledge the message on the server *before*
delivering it to the client.

This extra mode is supported by Apache ActiveMQ Artemis and will call it
*pre-acknowledge* mode.

The disadvantage of acknowledging on the server before delivery is that
the message will be lost if the system crashes *after* acknowledging the
message on the server but *before* it is delivered to the client. In
that case, the message is lost and will not be recovered when the system
restart.

Depending on your messaging case, `preAcknowledgement` mode can avoid
extra network traffic and CPU at the cost of coping with message loss.

An example of a use case for pre-acknowledgement is for stock price
update messages. With these messages it might be reasonable to lose a
message in event of crash, since the next price update message will
arrive soon, overriding the previous price.

> **Note**
>
> Please note, that if you use pre-acknowledge mode, then you will lose
> transactional semantics for messages being consumed, since clearly
> they are being acknowledged first on the server, not when you commit
> the transaction. This may be stating the obvious but we like to be
> clear on these things to avoid confusion!

## Using PRE_ACKNOWLEDGE

This can be configured by setting the boolean URL parameter `preAcknowledge`
to `true`.

Alternatively, when using the JMS API, create a JMS Session with the
`ActiveMQSession.PRE_ACKNOWLEDGE` constant.

    // messages will be acknowledge on the server *before* being delivered to the client
    Session session = connection.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE);

## Individual Acknowledge

A valid use-case for individual acknowledgement would be when you need
to have your own scheduling and you don't know when your message
processing will be finished. You should prefer having one consumer per
thread worker but this is not possible in some circumstances depending
on how complex is your processing. For that you can use the individual
acknowledgement.

You basically setup Individual ACK by creating a session with the
acknowledge mode with `ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE`.
Individual ACK inherits all the semantics from Client Acknowledge, with
the exception the message is individually acked.

> **Note**
>
> Please note, that to avoid confusion on MDB processing, Individual
> ACKNOWLEDGE is not supported through MDBs (or the inbound resource
> adapter). this is because you have to finish the process of your
> message inside the MDB.

## Example

See the [examples](examples.md) chapter for an example which shows how to
use pre-acknowledgement mode with JMS.
