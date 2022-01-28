# Symmetric Simple Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.

This example demonstrates how data is partitioned across two brokers. The idea is to focus on
'data gravity' and partition connections to brokers based on the data they will access,
rather than focus on balancing connection numbers overall.
For example, ensuring that all users of some particular set of data map to `Broker0`
and all users of some other set, map to `Broker1`. In this way, there is no extra movement of
data to service requests.
This architecture is intentionally quite static; based on the configured regular expressions,
it is always possible to infer where a given application (key) should be routed to.

In this example both brokers have the role of target, but for a subset of keys. If the key is a match
it is accepted, if not it is rejected.
Note: redirection in this scenario is optional, with a round-robin distribution of client urls, a connection will
eventually find a local target match.
In this example, the qpid jms amqp client failover feature does the required round-robin distribution.

The job of application developers in this scenario is to provide a key that can easily be mapped to a regular
expression that can capture an appropriate 'center of data gravity' for a broker.

In configuration, the `local-target-filter` provides the regular expression that controls what keys are mapped to a broker.
`Broker0` takes clientIDs with prefix FOO and `Broker1` takes prefix BAR. The `key-filter` specifies how the key is extracted,
we care about the first 3 characters, from the user supplied clientId.

      <connection-router name="symmetric-simple">
            <key-type>CLIENT_ID</key-type>
            <key-filter>^.{3}</key-filter>
            <local-target-filter>^FOO.*</local-target-filter>
      </connection-router>
