# JMS Topic Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to send and receive a message to a JMS Topic with ActiveMQ Artemis.

Topics are a standard part of JMS, please consult the JMS 1.1 specification for full details.

A Topic is used to send messages using the publish-subscribe model, from a producer to 1 or more consumers.