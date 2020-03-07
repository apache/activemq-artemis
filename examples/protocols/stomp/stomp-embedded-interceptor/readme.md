# Stomp Embedded Interceptor Example

To run the example, simply type **mvn verify -Pexample** from this directory.

This example shows you how to configure ActiveMQ Artemis to intercept received Stomp messages.

The client will open a socket to initiate a Stomp 1.2 connection and then send one Stomp message (using TCP directly). The interceptor will print each message received.