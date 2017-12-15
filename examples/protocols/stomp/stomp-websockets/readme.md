# Stomp WebSockets Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis to send and receive Stomp messages from modern web browser using Web Sockets.

The example will start a ActiveMQ Artemis broker configured with Stomp over Web Sockets and JMS. Web browsers clients and Java application will exchange message using a JMS Topic.

## Example step-by-step

To subscribe to the topic from your web browser, open the [Chat Example](chat/index.html) from another tab. The chat example is preconfigured to connect to the ActiveMQ Artemis broker with the URL `ws://localhost:61613` and subscribe to the JMS Topic (through its core address `chat`).

You can open as many Web clients as you want and they will all exchange messages through the topic

## Documentation

A JavaScript library is used on the browser side to be able to use Stomp Over Web Sockets (please see its [documentation](http://jmesnil.net/stomp-websocket/doc/) for a complete description).