# Properties for Copied Messages

There are several operations within the broker that result in copying a
message. These include:

- Diverting a message from one address to another.
- Moving an expired message from a queue to the configured `expiry-address`
- Moving a message which has exceeded its `max-delivery-attempts` from a queue
  to the configured `dead-letter-address`
- Using the management API to administratively move messages from one queue to
  another

When this happens the body and properties of the original message are copied to
a new message. However, the copying process removes some potentially important
pieces of data so those are preserved in the following special message
properties:

- `_AMQ_ORIG_ADDRESS`

  a String property containing the *original address* of the message

- `_AMQ_ORIG_QUEUE`

  a String property containing the *original queue* of the message

- `_AMQ_ORIG_MESSAGE_ID`

  a String property containing the *original message ID* of the message

It's possible for the aforementioned operations to be combined. For example, a
message may be diverted from one address to another where it lands in a queue
and a consumer tries & fails to consume it such that the message is then sent
to a dead-letter address. Or a message may be administratively moved from one
queue to another where it then expires.

In cases like these the `ORIG` properties will contain the information from the
_last_ (i.e. most recent) operation.