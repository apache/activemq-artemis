# Filter Expressions

Apache ActiveMQ Artemis provides a powerful filter language based on a subset of the
SQL 92 expression syntax.

It is the same as the syntax used for JMS selectors, but the predefined
identifiers are different. For documentation on JMS selector syntax
please the JMS javadoc for
[javax.jms.Message](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html).

Filter expressions are used in several places in Apache ActiveMQ Artemis

- Predefined Queues. When pre-defining a queue, in
  `broker.xml` in either the core or jms configuration a filter
  expression can be defined for a queue. Only messages that match the
  filter expression will enter the queue.

- Core bridges can be defined with an optional filter expression, only
  matching messages will be bridged (see [Core Bridges](core-bridges.md)).

- Diverts can be defined with an optional filter expression, only
  matching messages will be diverted (see [Diverts](diverts.md)).

- Filter are also used programmatically when creating consumers,
  queues and in several places as described in [management](management.md).

There are some differences between JMS selector expressions and Apache ActiveMQ Artemis
core filter expressions. Whereas JMS selector expressions operate on a
JMS message, Apache ActiveMQ Artemis core filter expressions operate on a core message.

The following identifiers can be used in a core filter expressions to
refer to attributes of the core message in an expression:

- `AMQPriority`. To refer to the priority of a message. Message
  priorities are integers with valid values from `0 - 9`. `0` is the
  lowest priority and `9` is the highest. E.g.
  `AMQPriority = 3 AND animal = 'aardvark'`

- `AMQExpiration`. To refer to the expiration time of a message. The
  value is a long integer.

- `AMQDurable`. To refer to whether a message is durable or not. The
  value is a string with valid values: `DURABLE` or `NON_DURABLE`.

- `AMQTimestamp`. The timestamp of when the message was created. The
  value is a long integer.

- `AMQSize`. The size of a message in bytes. The value is an integer.

Any other identifiers used in core filter expressions will be assumed to
be properties of the message.

The JMS spec states that a String property should not get converted to a 
numeric when used in a selector. So for example, if a message has the `age` 
property set to String `21` then the following selector should not match 
it: `age > 18`. Since Apache ActiveMQ Artemis supports STOMP clients which
can only send messages with string properties, that restriction is a bit 
limiting. Therefore, if you want your filter expressions to auto-convert String 
properties to the appropriate number type, just prefix it with
`convert_string_expressions:`. If you changed the filter expression in the
previous example to be `convert_string_expressions:age > 18`, then it would 
match the aforementioned message.

The JMS spec also states that property identifiers (and therefore the
identifiers which are valid for use in a filter expression) are an: 

> unlimited-length sequence of letters and digits, the first of which must be
> a letter. A letter is any character for which the method 
> `Character.isJavaLetter` returns `true`. This includes `_` and `$`. A letter
> or digit is any character for which the method `Character.isJavaLetterOrDigit`
> returns `true`.
 
This constraint means that hyphens (i.e. `-`) cannot be used.
However, this constraint can be overcome by using the `hyphenated_props:` 
prefix. For example, if a message had the `foo-bar` property set to `0` then
the filter expression `hyphenated_props:foo-bar = 0` would match it.

## XPath

Apache ActiveMQ Artemis also supports special [XPath](https://en.wikipedia.org/wiki/XPath)
filters which operate on the *body* of a message. The body must be XML. To
use an XPath filter use this syntax:
```
XPATH '<xpath-expression>'
```

XPath filters are supported with and between producers and consumers using
the following protocols:

 - OpenWire JMS 
 - Core (and Core JMS)
 - STOMP
 - AMQP

Since XPath applies to the body of the message and requires parsing of XML
**it may be significantly slower** than normal filters.

Large messages are **not** supported.

The XML parser used for XPath is configured with these default "features":

 - `http://xml.org/sax/features/external-general-entities`: `false`
 - `http://xml.org/sax/features/external-parameter-entities`: `false`
 - `http://apache.org/xml/features/disallow-doctype-decl`: `true`

However, in order to deal with any implementation-specific issues the features
can be customized by using system properties starting with the
`org.apache.activemq.documentBuilderFactory.feature:` prefix, e.g.:
```
-Dorg.apache.activemq.documentBuilderFactory.feature:http://xml.org/sax/features/external-general-entities=true
```
