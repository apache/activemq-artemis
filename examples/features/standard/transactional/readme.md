# JMS Transactional Session Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to use a transacted Session with ActiveMQ Artemis.

Firstly 2 messages are sent via the transacted sending session before being committed. This ensures that both message are sent

Secondly the receiving session receives the messages firstly demonstrating a message being redelivered after the session being rolled back and then acknowledging receipt of the messages via the commit method.