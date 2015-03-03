# Filter Expressions

Apache ActiveMQ provides a powerful filter language based on a subset of the
SQL 92 expression syntax.

It is the same as the syntax used for JMS selectors, but the predefined
identifiers are different. For documentation on JMS selector syntax
please the JMS javadoc for
[javax.jms.Message](http://docs.oracle.com/javaee/6/api/javax/jms/Message.html).

Filter expressions are used in several places in Apache ActiveMQ

-   Predefined Queues. When pre-defining a queue, in
    `activemq-configuration.xml` in either the core or jms configuration a filter
    expression can be defined for a queue. Only messages that match the
    filter expression will enter the queue.

-   Core bridges can be defined with an optional filter expression, only
    matching messages will be bridged (see [Core Bridges](core-bridges.md)).

-   Diverts can be defined with an optional filter expression, only
    matching messages will be diverted (see [Diverts](diverts.md)).

-   Filter are also used programmatically when creating consumers,
    queues and in several places as described in [management](management.md).

There are some differences between JMS selector expressions and Apache ActiveMQ
core filter expressions. Whereas JMS selector expressions operate on a
JMS message, Apache ActiveMQ core filter expressions operate on a core message.

The following identifiers can be used in a core filter expressions to
refer to attributes of the core message in an expression:

-   `HQPriority`. To refer to the priority of a message. Message
    priorities are integers with valid values from `0 - 9`. `0` is the
    lowest priority and `9` is the highest. E.g.
    `HQPriority = 3 AND animal = 'aardvark'`

-   `HQExpiration`. To refer to the expiration time of a message. The
    value is a long integer.

-   `HQDurable`. To refer to whether a message is durable or not. The
    value is a string with valid values: `DURABLE` or `NON_DURABLE`.

-   `HQTimestamp`. The timestamp of when the message was created. The
    value is a long integer.

-   `HQSize`. The size of a message in bytes. The value is an integer.

Any other identifiers used in core filter expressions will be assumed to
be properties of the message.
