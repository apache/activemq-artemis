# Mapping JMS Concepts to the Core API

This chapter describes how JMS destinations are mapped to ActiveMQ
addresses.

ActiveMQ core is JMS-agnostic. It does not have any concept of a JMS
topic. A JMS topic is implemented in core as an address (the topic name)
with zero or more queues bound to it. Each queue bound to that address
represents a topic subscription. Likewise, a JMS queue is implemented as
an address (the JMS queue name) with one single queue bound to it which
represents the JMS queue.

By convention, all JMS queues map to core queues where the core queue
name has the string `jms.queue.` prepended to it. E.g. the JMS queue
with the name "orders.europe" would map to the core queue with the name
"jms.queue.orders.europe". The address at which the core queue is bound
is also given by the core queue name.

For JMS topics the address at which the queues that represent the
subscriptions are bound is given by prepending the string "jms.topic."
to the name of the JMS topic. E.g. the JMS topic with name "news.europe"
would map to the core address "jms.topic.news.europe"

In other words if you send a JMS message to a JMS queue with name
"orders.europe" it will get routed on the server to any core queues
bound to the address "jms.queue.orders.europe". If you send a JMS
message to a JMS topic with name "news.europe" it will get routed on the
server to any core queues bound to the address "jms.topic.news.europe".

If you want to configure settings for a JMS Queue with the name
"orders.europe", you need to configure the corresponding core queue
"jms.queue.orders.europe":

    <!-- expired messages in JMS Queue "orders.europe" will be sent to the JMS Queue "expiry.europe" -->
    <address-setting match="jms.queue.orders.europe">
       <expiry-address>jms.queue.expiry.europe</expiry-address>
       ...
    </address-setting>
