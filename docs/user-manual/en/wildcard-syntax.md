# Wildcard Syntax

Apache ActiveMQ Artemis uses a specific syntax for representing wildcards in security
settings, address settings and when creating consumers.

The syntax is similar to that used by [AMQP](https://www.amqp.org).

An Apache ActiveMQ Artemis wildcard expression contains words delimited by the character
'`.`' (full stop).

The special characters '`#`' and '`*`' also have special meaning and can
take the place of a word.

The character '`#`' means 'match any sequence of zero or more words'.

The character '`*`' means 'match a single word'.

So the wildcard 'news.europe.\#' would match 'news.europe',
'news.europe.sport', 'news.europe.politics', and
'news.europe.politics.regional' but would not match 'news.usa',
'news.usa.sport' nor 'entertainment'.

The wildcard 'news.\*' would match 'news.europe', but not
'news.europe.sport'.

The wildcard 'news.\*.sport' would match 'news.europe.sport' and also
'news.usa.sport', but not 'news.europe.politics'.

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