![ActiveMQ Artemis logo](images/artemis-logo.jpg)

Apache ActiveMQ Artemis Migration Guide
=====================================

As more and more people start using Artemis, it's valuable to have a migration guide that will help experienced ActiveMQ users adapt to the new broker. From outside, two brokers might seem very similar, but there are subtle differences in their inner-workings that can lead to confusions. The goal of this guide is to explain these differences and help make a transition.

Migration is a fairly broad term in systems like these, so what are we talking about here? This guide will be focused only on broker server migration. We'll assume that the current system is a working ActiveMQ 5.x broker with OpenWire JMS clients. We'll see how we can replace the broker with Artemis and leave the clients intact. This guide will not cover a message store migration. That topic and aspects of migrating clients that use some other protocol will be the subject of future guides.

This guide is aimed at experienced ActiveMQ users that want to learn more about what's different in Artemis. We will assume that you know the concepts that are covered in these articles. They will not be explained from the first principles, for that you're advised to see appropriate manuals of the ActiveMQ and Artemis brokers.

Before we dig into more details on the migration, let's talk about basic conceptual differences between two brokers.

## Architectural differences

Although they are designed to do the same job, things are done differently internally. Here are some of the most notable architectural differences you need to be aware of when you're planning the migration.

In ActiveMQ, we have a few different implementations of the IO connectivity layer, like tcp (synchronous one) and nio (non-blocking one). In Artemis, the IO layer is implemented using Netty, which is a nio framework. This means that there's no more need to choose between different implementations as the non-blocking one is used by default.

The other important part of every broker is a message store. Most of the ActiveMQ users are familiar with KahaDB. It consists of a message journal for fast sequential storing of messages (and other command packets) and an index for retrieving messages when needed.

Artemis has its own message store. It consists only of the append-only message journal. Because of the differences in how paging is done, there's no need for the message index. We'll talk more about that in a minute. It's important to say at this point that these two stores are not interchangeable, and data migration if needed must be carefully planed.

What do we mean by paging differences? Paging is the process that happens when broker can't hold all incoming messages in its memory. The strategy of how to deal with this situation differs between two brokers. ActiveMQ have *cursors*, which are basically a cache of messages ready to be dispatched to the consumer. It will try to keep all incoming messages in there. When we run out of the the available memory, messages are added to the store, but the caching stops. When the space become available again, the broker will fill the cache again by pulling messages from the store in batches. Because of this, we need to read the journal from time to time during a broker runtime. In order to do that, we need to maintain a journal index, so that messages' position can be tracked inside the journal.

In Artemis, things work differently in this regard. The whole message journal is kept in memory and messages are dispatched directly from it. When we run out of memory, messages are paged *on the producer side* (before they hit the broker). Theay are stored in sequential page files in the same order as they arrived. Once the memory is freed, messages are moved from these page files into the journal. With paging working like this, messages are read from the file journal only when the broker starts up, in order to recreate this in-memory version of the journal. In this case, the journal is only read sequentially, meaning that there's no need to keep an index of messages in the journal.

This is one of the main differences between ActiveMQ 5.x and Artemis. It's important to understand it early on as it affects a lot of destination policy settings and how we configure brokers in order to support these scenarios properly. 

## Addressing differences

Another big difference that's good to cover early on is the difference is how message addressing and routing is done. ActiveMQ started as a open source JMS implementation, so at its core all JMS concepts like queues, topics and durable subscriptions are implemented as the first-class citizens. It's all based on OpenWire protocol developed within the project and even KahaDB message store is OpenWire centric. This means that all other supported protocols, like MQTT and AMQP are translated internally into OpenWire.

Artemis took a different approach. It implements only queues internally and all other messaging concepts are achieved by routing messages to appropriate queue(s) using addresses. Messaging concepts like publish-subscribe (topics) and point-to-point (queues) are implemented using different type of routing mechanisms on addresses. *Multicast* routing is used to implement *publish-subscribe* semantics, where all subscribers to a certain address will get their own internal queue and messages will be routed to all of them. *Anycast* routing is used implement *point-to-point* semantics, where there'll be only one queue for the address and all consumers will subscribe to it. The addressing and routing scheme is used across all protocols. So for example, you can view the JMS topic just as a multicast address. We'll cover this topic in more details in the later articles.



