# AMQP Broker Connection with Senders and SSL
 
To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to create and start the broker manually.
 
This example demonstrates how you can create a broker connection from one broker towards another broker, and send messages from that broker towards the target server.
 
You basically configured the broker connection on broker.xml and this example will give you two working servers where you send messages in one broker and receive it on another broker.
 
The connection between the two brokers as well as the client connections are all configured to use SSL.

The keystore and trustores used in the example were generated with store-generation.txt
