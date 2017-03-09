# Mapping JMS Concepts to the Core API

This chapter describes how JMS destinations are mapped to Apache ActiveMQ Artemis
addresses.

Apache ActiveMQ Artemis core is JMS-agnostic. It does not have any concept of a JMS
topic. A JMS topic is implemented in core as an address with name=(the topic name) 
and with a MULTICAST routing type with zero or more queues bound to it. Each queue bound to that address
represents a topic subscription. 

Likewise, a JMS queue is implemented as an address with name=(the JMS queue name) with an ANYCAST routing type assocatied
with it.

Note.  That whilst it is possible to configure a JMS topic and queue with the same name, it is not a recommended
configuration for use with cross protocol. 
