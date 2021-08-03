# AMQP Broker Disaster Recovery
 
To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.
 
On this broker you will have two brokers connected to each other.

This broker configure two servers:

- server0
- server1

Each broker has a broker connection towards the other broker, with a mirror tag configured:

```xml
 <broker-connections>
    <amqp-connection uri="tcp://localhost:5661" name="otherBroker" retry-interval="1000">
       <mirror/>
    </amqp-connection>
</broker-connections>
```


Watever happens in server0 is replicated to server1 and vice versa.

In case you want to play this in real life, all you have to do is to move your consumers and producers from one broker towards the other broker.