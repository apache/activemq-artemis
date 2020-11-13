# Retroactive Addresses

A "retroactive" address is an address that will preserve messages sent to it
for queues which will be created on it in the future. This can be useful in,
for example, publish-subscribe use cases where clients want to receive the
messages sent to the address *before* they actually connected and created
their multicast "subscription" queue. Typically messages sent to an address
before a queue was created on it would simply be unavailable to those queues,
but with a retroactive address a fixed number of messages can be preserved by
the broker and automatically copied into queues subsequently created on the
address. This works for both anycast and multicast queues.

## Internal Retroactive Resources

To implement this functionality the broker will create 4 internal resources for
each retroactive address:

1. A non-exclusive [divert](#diverts) to grab the messages from the retroactive
   address.
2. An address to receive the messages from the divert.
3. **Two** [ring queues](#ring-queues) to hold the messages sent to the address
   by the divert - one for anycast and one for multicast. The general caveats
   for ring queues still apply here. See [the chapter on ring queues](#ring-queues)
   for more details.

These resources are important to be aware of as they will show up in the web
console and other management or metric views. They will be named according to
the following pattern:

```
<internal-naming-prefix><delimiter><source-address><delimiter>(divert|address|queue<delimiter>(anycast|multicast))<delimiter>retro
```

For example, if an address named `myAddress` had a `retroactive-message-count`
of 10 and the default `internal-naming-prefix` (i.e. `$.artemis.internal.`) and
the default delimiter (i.e. `.`) were being used then resources with these names
would be created:

1. A divert on `myAddress` named `$.artemis.internal.myAddress.divert.retro`
2. An address named `$.artemis.internal.myAddress.address.retro`
3. A multicast queue on the address from step #2 named
   `$.artemis.internal.myAddress.queue.multicast.retro` with a `ring-size` of 10.
4. An anycast queue on the address from step #2 named
   `$.artemis.internal.myAddress.queue.anycast.retro` with a `ring-size` of 10.

This pattern is important to note as it allows one to configure address-settings
if necessary. To configure custom address-settings you'd use a match like:

```
*.*.*.<source-address>.*.retro
```

Using the same example as above the `match` would be:

```
*.*.*.myAddress.*.retro
```

> Note:
>
> Changing the broker's `internal-naming-prefix` once these retroactive
> resources are created will break the retroactive functionality.
>

## Configuration

To configure an address to be "retroactive" simply configure the
`retroactive-message-count` `address-setting` to reflect the number of messages
you want the broker to preserve, e.g.:


```xml
<address-settings>
   <address-setting match="orders">
      <retroactive-message-count>100</retroactive-message-count>
   </address-setting>
</address-settings>
```

The value for `retroactive-message-count` can be updated at runtime either via
`broker.xml` or via the management API just like any other address-setting.
However, if you *reduce* the value of `retroactive-message-count` an additional
administrative step will be required since this functionality is implemented
via ring queues. This is because a ring queue whose ring-size is reduced will
not automatically delete messages from the queue to meet the new ring-size in
order to avoid unintended message loss. Therefore, administrative action will
be required in this case to manually reduce the number of messages in the ring
queue via the management API.
