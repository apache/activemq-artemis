# JMS QueueRequestor Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to use a [QueueRequestor](https://docs.oracle.com/javaee/7/api/javax/jms/QueueRequestor.html) with ActiveMQ Artemis.

JMS is mainly used to send messages asynchronously so that the producer of a message is not waiting for the result of the message consumption. However, there are cases where it is necessary to have a synchronous behavior: the code sending a message requires a reply for this message before continuing its execution.
A QueueRequestor facilitates this use case by providing a simple request/reply abstraction on top of JMS.

The example consists in two classes:

* `TextReverserService`: A JMS MessageListener which consumes text messages and sends replies containing the reversed text.

* `QueueRequestorExample`: A JMS Client which uses a QueueRequestor to send text requests to a queue and receive replies with the reversed text in a synchronous fashion.