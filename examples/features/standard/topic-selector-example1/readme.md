# JMS Topic Selector Example 1

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how messages can be consumed from a topic using Message Selectors.

Consumers (or Subscribers) will only consume messages routed to a topic that match the provided selector

Topics and selectors are a standard part of JMS, please consult the JMS 1.1 specification for full details.