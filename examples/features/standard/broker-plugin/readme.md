# Broker Plugin Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

AMQP Messages should be by definition immutable at the server's. So, we don't recommend changing message contents. However if you require you can use this example as a basis for adding properties on making changes