# Exclusive Queues

Exclusive queues are special queues which route all messages to only one 
consumer at a time.

This is useful when you want all messages to be processed serially by the same 
consumer, when a producer does not specify [Message Grouping](message-grouping.md).

An example might be orders sent to an address and you need to consume them 
in the exact same order they were produced.

Obviously exclusive queues have a draw back that you cannot scale out the 
consumers to improve consumption as only one consumer would technically be active. 
Here we advise that you look at message groups first.


## Configuring Exclusive Queues

Exclusive queues can be statically configured using the `exclusive` boolean 
property:

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" exclusive="true"/>
   </multicast>
</address>
```

Specified on creating a Queue by using the CORE api specifying the parameter 
`exclusive` to `true`. 

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?exclusive=true");
Topic topic = session.createTopic("my.destination.name?exclusive=true");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="lastValueQueue">
   <default-exclusive-queue>true</default-exclusive-queue>
</address-setting>
```

By default, `default-exclusive-queue` is `false`. Address 
[wildcards](wildcard-syntax.md) can be used to configure exclusive queues for a 
set of addresses.


## Example

See the [exclusive queue example](examples.md#exclusive) which shows how 
exclusive queues are configured and used with JMS.
