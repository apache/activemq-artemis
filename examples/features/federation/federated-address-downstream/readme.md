# Federated Address Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a core multicast address deployed on three different brokers. The three brokers are configured to form a federated address mesh.

In the example we name the brokers, eu-west, eu-east and us-central to give an idea of the use case.

![EU West, EU East and US Central Diagram](eu-west-east-us-central.png)

The following is then carried out:

1. create a consumer on the queue on each node, and we create a producer on only one of the nodes.

2. send some messages via the producer on EU West, and we verify that **all** the consumers receive the sent messages, in essence multicasting the messages within and accross brokers.

3. Next then verify the same on US Central.

4. Next then verify the same on EU East.



In other words, we are showing how with Federated Address, ActiveMQ Artemis **replicates**  sent messages to all addresses and subsequently delivered to all consumers, regardless if the consumer is local or is on a distant broker. Decoupling the location where producers and consumers need to be.

The config that defines the federation you can see in the broker.xml for each broker is within the following tags. You will note upstreams are different in each as well as the federation name, which has to be globally unique.

```
 <federations>
    ...
 </federations>
```


For more information on ActiveMQ Artemis Federation please see the federation section of the user manual.
