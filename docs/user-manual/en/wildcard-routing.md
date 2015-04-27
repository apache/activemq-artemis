# Routing Messages With Wild Cards

Apache ActiveMQ Artemis allows the routing of messages via wildcard addresses.

If a queue is created with an address of say `queue.news.#` then it will
receive any messages sent to addresses that match this, for instance
`queue.news.europe` or `queue.news.usa` or `queue.news.usa.sport`. If
you create a consumer on this queue, this allows a consumer to consume
messages which are sent to a *hierarchy* of addresses.

> **Note**
>
> In JMS terminology this allows "topic hierarchies" to be created.

To enable this functionality set the property
`wild-card-routing-enabled` in the `activemq-configuration.xml` file to
`true`. This is `true` by default.

For more information on the wild card syntax take a look at [wildcard syntax](wildcard-syntax.md) chapter,
also see the topic hierarchy example in the [examples](examples.md).
