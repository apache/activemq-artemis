# Evenly Redirect Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.

This example demonstrates how incoming client connections are evenly redirected across two brokers
using a third broker with a connection router to redirect incoming client connections,
based on a least-connections policy and caching on a filtered prefix of the connection ClientID.

