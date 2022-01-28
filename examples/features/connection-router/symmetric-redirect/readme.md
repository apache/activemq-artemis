# Symmetric Redirect Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.

This example demonstrates how incoming client connections are distributed across two brokers
using a symmetric architecture. In this architecture both brokers have two roles: router and target.
So they can redirect or accept the incoming client connection according to the consistent hash algorithm.
Both brokers use the same consistent hash algorithm to select the target broker so for the same key
if the first broker redirects an incoming client connection the second accepts it and vice versa.