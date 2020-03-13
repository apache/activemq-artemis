# Breadcrumb Message Properties

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

This process results in several copies and several different `ORIG` properties.
To preserve the entire history any existing values for `_AMQ_ORIG_ADDRESS`,
`_AMQ_ORIG_QUEUE`, and `_AMQ_ORIG_MESSAGE_ID` are copied to new properties
using their original name suffixed with `_` plus an ascending numerical value
starting at 0.

Expanding on the first use-case described above, consider a client who sends a
message to address `A` (to which the broker assigns `123` as the message ID).
The broker has a `divert` named `divertAtoB` which takes the message sent to
address `A` and forwards it to address `B` where it lands in the anycast queue
`X`. When this happens the message will have a new message ID of `456` and the
following `ORIG` properties:

- `_AMQ_ORIG_ADDRESS`: `A`
- `_AMQ_ORIG_QUEUE`: `divertAtoB` (i.e. the name of the `divert`)
- `_AMQ_ORIG_MESSAGE_ID`: `123`

Now consider a consumer which receives the message from queue `X`, but (for
whatever reason) keeps calling `rollback()` on its session forcing redeliveries
until the message is sent to the dead-letter address `DLA`. When this happens
the message will have a new message ID of `456` and the following `ORIG`
properties:

- `_AMQ_ORIG_ADDRESS`: `B`
- `_AMQ_ORIG_QUEUE`: `X`
- `_AMQ_ORIG_MESSAGE_ID`: `456`
- `_AMQ_ORIG_ADDRESS_0`: `A`
- `_AMQ_ORIG_QUEUE_0`: `divertAtoB`
- `_AMQ_ORIG_MESSAGE_ID_0`: `123`

Using these "bread crumb" properties a message can be tracked through each
different step.