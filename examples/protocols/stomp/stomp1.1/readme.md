# Stomp 1.1 Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis to send and receive Stomp messages using Stomp 1.1 protocol.

The client will open a socket to initiate a Stomp 1.1 connection and then send one Stomp message (using TCP directly). The client will then consume a message from a queue and check it is the message sent with Stomp.