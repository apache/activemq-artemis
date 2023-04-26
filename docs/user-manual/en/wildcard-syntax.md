# Wildcard Syntax

Apache ActiveMQ Artemis uses a specific syntax for representing
wildcards in security settings, address settings and when creating
consumers.

The syntax is similar to that used by [AMQP](https://www.amqp.org).

An Apache ActiveMQ Artemis wildcard expression contains **words
separated by a delimiter**. The default delimiter is `.` (full stop).

The special characters `#` and `*` also have special meaning and can
take the place of a **word**.

To be clear, the wildcard characters cannot be used like wildcards in
a [regular expression](https://en.wikipedia.org/wiki/Regular_expression).
They operate exclusively on **words separated by a delimiter**.

## Matching Any Word

The character `#` means "match any sequence of zero or more words".

So the wildcard `news.europe.#` would match:
 - `news.europe`
 - `news.europe.sport`
 - `news.europe.politics`
 - `news.europe.politics.regional`

But `news.europe.#` would _not_ match:
 - `news.usa`
 - `news.usa.sport`
 - `entertainment`

## Matching a Single Word

The character `*` means "match a single word".

The wildcard `news.*` would match:
 - `news.europe`
 - `news.usa`

But `news.*` would _not_ match:
 - `news.europe.sport`
 - `news.usa.sport`
 - `news.europe.politics.regional`

The wildcard `news.*.sport` would match:
 - `news.europe.sport`
 - `news.usa.sport`

But `news.*.sport` would _not_ match:
 - `news.europe.politics`

## Customizing the Syntax

It's possible to further configure the syntax of the wildcard addresses using the broker configuration. 
For that, the `<wildcard-addresses>` configuration tag is used.

```xml
<wildcard-addresses>
   <routing-enabled>true</routing-enabled>
   <delimiter>.</delimiter>
   <any-words>#</any-words>
   <single-word>*</single-word>
</wildcard-addresses>
```

The example above shows the default configuration.