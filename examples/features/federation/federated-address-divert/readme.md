# Federated Address Divert Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a core multicast address deployed on two different brokers. The two brokers are configured to form a federated address mesh.

In the example we name the brokers eu-west and eu-east.

The following is then carried out:

1. create a divert binding with a source address of exampleTopic and target address of divertExampleTopic on eu-west

1. create a consumer on the topic divertExampleTopic on eu-west and create a producer on the topic exampleTopic on eu-east.

2. send some messages via the producer on eu-east, and we verify the eu-west consumer receives the messages because of the divert binding demand


For more information on ActiveMQ Artemis Federation please see the federation section of the user manual.