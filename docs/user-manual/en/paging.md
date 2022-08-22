# Paging

Apache ActiveMQ Artemis transparently supports huge queues containing millions
of messages while the server is running with limited memory.

In such a situation it's not possible to store all of the queues in memory at
any one time, so Apache ActiveMQ Artemis transparently *pages* messages into
and out of memory as they are needed, thus allowing massive queues with a low
memory footprint.

Apache ActiveMQ Artemis will start paging messages to disk, when the size of
all messages in memory for an address exceeds a configured maximum size.

The default configuration from Artemis has destinations with paging.

## Page Files

Messages are stored per address on the file system. Each address has an
individual folder where messages are stored in multiple files (page files).
Each file will contain messages up to a max configured size
(`page-size-bytes`). The system will navigate the files as needed, and it
will remove the page file as soon as all the messages are acknowledged up to
that point.

Browsers will read through the page-cursor system.

Consumers with selectors will also navigate through the page-files and it will
ignore messages that don't match the criteria.

> *Warning:*
>
> When you have a queue, and consumers filtering the queue with a very
> restrictive selector you may get into a situation where you won't be able to
> read more data from paging until you consume messages from the queue.
>
> Example: in one consumer you make a selector as 'color="red"' but you only
> have one color red 1 millions messages after blue, you won't be able to
> consume red until you consume blue ones.
>
> This is different to browsing as we will "browse" the entire queue looking
> for messages and while we "depage" messages while feeding the queue.



### Configuration

You can configure the location of the paging folder in `broker.xml`.

- `paging-directory` Where page files are stored. Apache ActiveMQ Artemis will
  create one folder for each address being paged under this configured
  location. Default is `data/paging`.

## Paging Mode

As soon as messages delivered to an address exceed the configured size,
that address alone goes into page mode. If max-size-bytes == 0 or max-size-messages == 0, an address
will always use paging to route messages.

> **Note:**
>
> Paging is done individually per address. If you configure a max-size-bytes or max-messages
> for an address, that means each matching address will have a maximum size
> that you specified. It DOES NOT mean that the total overall size of all
> matching addresses is limited to max-size-bytes. Use [global-max-size](#global-max-size) or [global-max-messages](#global-max-messages) for that!

### Configuration

Configuration is done at the address settings in `broker.xml`.

```xml
<address-settings>
   <address-setting match="jms.someaddress">
      <max-size-bytes>104857600</max-size-bytes>
      <max-size-messages>1000</max-size-messages>
      <page-size-bytes>10485760</page-size-bytes>
      <address-full-policy>PAGE</address-full-policy>
   </address-setting>
</address-settings>
```

> **Note:**
> The [management-address](management.md#configuring-management)
> settings cannot be changed or overridden ie management
> messages aren't allowed to page/block/fail and are considered
> an internal broker management mechanism.
> The memory occupation of the [management-address](management.md#configuring-management)
> is not considered while evaluating if [global-max-size](#global-max-size)
> is hit and can't cause other non-management addresses to trigger a
> configured `address-full-policy`.
 
This is the list of available parameters on the address settings.

Property Name|Description|Default
---|---|---
`max-size-bytes`|What's the max memory the address could have before entering on page mode.|-1 (disabled)
`max-size-messages`|The max number of messages the address could have before entering on page mode.| -1 (disabled)
`page-size-bytes`|The size of each page file used on the paging system|10MB
`address-full-policy`|This must be set to `PAGE` for paging to enable. If the value is `PAGE` then further messages will be paged to disk. If the value is `DROP` then further messages will be silently dropped. If the value is `FAIL` then the messages will be dropped and the client message producers will receive an exception. If the value is `BLOCK` then client message producers will block when they try and send further messages.|`PAGE`
`max-read-page-messages` | how many message can be read from paging into the Queue whenever more messages are needed. The system wtill stop reading if `max-read-page-bytes hits the limit first. | -1
`max-read-page-bytes` | how much memory the messages read from paging can take on the Queue whenever more messages are needed. The system will stop reading if `max-read-page-messages` hits the limit first. | 2 * page-size-bytes

### max-size-bytes and max-size-messages simultaneous usage

It is possible to define max-size-messages (as the maximum number of messages) and max-messages-size (as the max number of estimated memory used by the address) concurrently. The configured policy will start based on the first value to reach its mark.

#### Maximum read from page

`max-read-page-messages` and `max-read-page-bytes` are used to control messaging reading from paged file into the Queue. The broker will add messages on the Queue until either `max-read-page-meessages` or `max-read-page-bytes` reaches the limit.

If both values are set to -1 the broker will keep reading messages as long as the consumer is reaching for more messages. However this would keep the broker unprotected from consumers allocating huge transactions or consumers that don't have flow control enabled.

## Global Max Size

Beyond the `max-size-bytes` on the address you can also set the global-max-size
on the main configuration. If you set `max-size-bytes` = `-1` on paging the
`global-max-size` can still be used.

## Global Max Messages

You can also specify `global-max-messages` on the main configuration, specifying how many messages the system would accept before entering into the configured full policy mode configured.

When you have more messages than what is configured `global-max-size` any new
produced message will make that destination to go through its paging policy. 

`global-max-size` is calculated as half of the max memory available to the Java
Virtual Machine, unless specified on the `broker.xml` configuration.

By default `global-max-messages` = `-1` meaning it's disabled.


## Dropping messages

Instead of paging messages when the max size is reached, an address can also be
configured to just drop messages when the address is full.

To do this just set the `address-full-policy` to `DROP` in the address settings

## Dropping messages and throwing an exception to producers

Instead of paging messages when the max size is reached, an address can also be
configured to drop messages and also throw an exception on the client-side when
the address is full.

To do this just set the `address-full-policy` to `FAIL` in the address settings

## Blocking producers

Instead of paging messages when the max size is reached, an address can also be
configured to block producers from sending further messages when the address is
full, thus preventing the memory being exhausted on the server.

When memory is freed up on the server, producers will automatically unblock and
be able to continue sending.

To do this just set the `address-full-policy` to `BLOCK` in the address
settings

In the default configuration, all addresses are configured to block producers
after 10 MiB of data are in the address.

## Caution with Addresses with Multiple Multicast Queues

When a message is routed to an address that has multiple multicast queues bound
to it, e.g. a JMS subscription in a Topic, there is only 1 copy of the message
in memory. Each queue only deals with a reference to this.  Because of this the
memory is only freed up once all queues referencing the message have delivered
it.

If you have a single lazy subscription, the entire address will suffer IO
performance hit as all the queues will have messages being sent through an
extra storage on the paging system.

For example:

- An address has 10 multicast queues

- One of the queues does not deliver its messages (maybe because of a
  slow consumer).

- Messages continually arrive at the address and paging is started.

- The other 9 queues are empty even though messages have been sent.

In this example all the other 9 queues will be consuming messages from the page
system. This may cause performance issues if this is an undesirable state.

## Max Disk Usage

The System will perform scans on the disk to determine if the disk is beyond a
configured limit.  These are configured through `max-disk-usage` in percentage.
Once that limit is reached any message will be blocked. (unless the protocol
doesn't support flow control on which case there will be an exception thrown
and the connection for those clients dropped).

## Page Sync Timeout

The pages are synced periodically and the sync period is configured through
`page-sync-timeout` in nanoseconds. When using NIO journal, by default has
the same value of `journal-buffer-timeout`. When using ASYNCIO, the default
should be `3333333`.

## Memory usage from Paged Messages.

The system should keep at least one paged file in memory caching ahead reading messages. 
Also every active subscription could keep one paged file in memory. 
So, if your system has too many queues it is recommended to minimize the page-size.

## Example

See the [Paging Example](examples.md#paging) which shows how to use paging with 
Apache ActiveMQ Artemis.
