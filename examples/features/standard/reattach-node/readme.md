# JMS Reattach Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates how ActiveMQ Artemis connections can be configured to be resilient to temporary network failures.

In the case of a network failure being detected, either as a result of a failure to read/write to the connection, or the failure of a pong to arrive back from the broker in good time after a ping is sent, instead of failing the connection immediately and notifying any user ExceptionListener objects, ActiveMQ can be configured to automatically retry the connection, and reattach to the broker when it becomes available again across the network.

When the client reattaches to the broker it will be able to resume using its sessions and connections where it left off

This is different to client reconnect as the sessions, consumers etc still exist on the server. With reconnect The client recreates its sessions and consumers as needed.

This example starts a single server, connects to it and performs some JMS operations. We then simulate failure of the network connection by temporarily stopping the network acceptor on the server. (This is done by sending management messages, but that is not central to the purpose of the example).

We then wait a few seconds, then restart the acceptor. The client reattaches and the session resumes as if nothing happened.

The JMS Connection Factory is configured to reattach automatically by specifying the various reconnect related attributes in the connection URL in `jndi.properties`.

For more details on how to configure this and for clustering in general please consult the ActiveMQ Artemis user manual.