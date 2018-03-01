# MQTT Publish/Subscribe Example

This is a basic MQTT example that demonstrates how to setup and connect to an Apache Artemis broker and send and receive messages using the MQTT protocol.

## Setting up the Broker

This example will use the default out of the box configuration of Artemis you don't need to change anything to run this example. Artemis ships with all protocols enabled on port 61616 and also MQTT on port 1883. To enable MQTT on a different port you can add the following XML snippet to the `acceptors` section of your broker.xml configuration file (changing the port from 1883 to what ever you require).

    <acceptor name="mqtt">tcp://0.0.0.0:1883?protocols=MQTT"></acceptor>

For more information on configuring protocol transports see the "Configuring Transports" section of the user manual, specifically the section called "Single Port Support".

## MQTT Clients

There are a number of MQTT client implementations for various languages. The Paho project: https://www.eclipse.org/paho/ offers a number of clients for languages such as C, Python, JavaScript and .Net and is also a great resource for all things MQTT. This example is actually based on the Fuse MQTT java client and was chosen as it is Apache 2.0 licensed and available to download from maven central. The specific client used in the example is not of importance and is used simply to demonstrate the features of MQTT as provided by Apache Artemis.

If you'd like to use the client demonstrated in this example, simple add the following dependency to your pom.xml

    <dependency>
       <groupId>org.fusesource.mqtt-client</groupId>
       <artifactId>mqtt-client</artifactId>
       <version>1.10</version>
    </dependency>

## Example Step by Step

* Connect to Artemis

We start by creating a connection to the Apache Artemis broker. In this example we specify to use TCP protocol on localhost. By default Apache Artemis will start all protocols on port 61616, so we connect to that port.

    MQTT mqtt = new MQTT();
    mqtt.setHost("tcp://localhost:61616");
    BlockingConnection connection = mqtt.blockingConnection();
    connection.connect();

* Create subscriptions

Subscriptions in MQTT are realised by subscribing to a particular Topic. Each Topic has an address and a quality of service level (QoS level). Subscriptions also support wildcards. In the code below we subscribe to a Topic with address "mqtt/example/publish", a wildcard address "test/#" which will match anything starting with "test/" and also a wildcard "foo/+/bar", where + matches a single level of the hierarchy (foo/something/bar)

    Topic[] topics = { new Topic("mqtt/example/publish", QoS.AT_LEAST_ONCE), new Topic("test/#", QoS.EXACTLY_ONCE), new Topic("foo/+/bar", QoS.AT_LEAST_ONCE) };
    connection.subscribe(topics);

* Sending messages

There is no type system in MQTT, messages simply consist of a number of bytes. Below we send three messages with UTF8 encoded strings (as a byte array). Notice the second message is sent to "test/test" which should match the first wildcard subscription we defined previously. The third message is sent to "foo/1/bar", which matches the second wildcard subscription.

    String payload1 = "This is message 1";
    String payload2 = "This is message 2";
    String payload3 = "This is message 3";

    connection.publish("mqtt/example/publish", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
    connection.publish("mqtt/test", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
    connection.publish("foo/1/bar", payload3.getBytes(), QoS.AT_MOST_ONCE, false);

* Receiving messages

Since we have subscribed to a number of topics and sent messages to them, the client should now receive 2 messages. We are not using callbacks here on message receive so we specifically call receive to get the messages. Once we receive the message we convert the payload consisting of bytes back to a UTF8 encoded string and print the result.
    
    Message message1 = connection.receive(5, TimeUnit.SECONDS);
    Message message2 = connection.receive(5, TimeUnit.SECONDS);
    Message message3 = connection.receive(5, TimeUnit.SECONDS);

    System.out.println(new String(message1.getPayload()));
    System.out.println(new String(message2.getPayload()));
    System.out.println(new String(message3.getPayload()));

## Result

This example has shown you how to set up the basics of MQTT including how to connect to the Artemis broker and how to send and receive messages including subscriptions using wildcard addresses.