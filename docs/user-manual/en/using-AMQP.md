# Using AMQP

Apache ActiveMQ Artemis is also a pure AMQP 1.0 broker, with a high performant and feature complete protocol manager for AMQP.

You can use *any* AMQP 1.0 compatible clients.

A short list includes:

- qpid clients at the [qpid project](http://qpid.apache.org/download.html)
- [.NET Clients](https://blogs.apache.org/activemq/entry/using-net-libraries-with-activemq)
- [Javascript NodeJS](https://github.com/noodlefrenzy/node-amqp10)
- [Java Script RHEA](https://github.com/grs/rhea)


... and many others.


# Message Conversions

The broker will not perform any message conversion to any other protocols when sending AMQP and receiving AMQP.

However if you intend your message to be received on a AMQP JMS Client, you must follow the JMS Mapping convention:

- [JMS Mapping Conventions](https://www.oasis-open.org/committees/download.php/53086/amqp-bindmap-jms-v1.0-wd05.pdf)


If you send a body type that is not recognized by this specification the conversion between AMQP and any other protocol will make it a Binary Message.

So, make sure you follow these conventions if you intend to cross protocols or languages. Especially on the message body.


# Example

We have a few examples as part of the Artemis distribution:


- .NET: 
 * ./examples/protocols/amqp/dotnet

- ProtonCPP
 * ./examples/protocols/amqp/proton-cpp
 
- Ruby
 * ./examples/protocols/amqp/proton-ruby
 
- Java (Using the qpid JMS Client)
 * ./examples/protocols/amqp/queue



