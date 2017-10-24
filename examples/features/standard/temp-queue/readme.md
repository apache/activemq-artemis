# JMS Temporary Queue Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to use a TemporaryQueue with ActiveMQ Artemis. First a temporary queue is created to send and receive a message and then deleted. Then another temporary queue is created and used after its connection is closed to illustrate its scope.

A TemporaryQueue is a JMS queue that exists only within the lifetime of its connection. It is often used in request-reply type messaging where the reply is sent through a temporary destination. The temporary queue is often created as a broker resource, so after using, the user should call delete() method to release the resources. Please consult the JMS 1.1 specification for full details.