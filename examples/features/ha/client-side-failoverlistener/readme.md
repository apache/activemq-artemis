# JMS Client Side Failover Listener Example

To run the example, simply type **mvn verify** from this directory. This example will always spawn and stop multiple servers.

This example demonstrates how you can listen on failover event on the client side.

In this example there are two nodes running in a cluster, both broker will be running for start, but after a while the first broker will crash. This will trigger a fail-over event.