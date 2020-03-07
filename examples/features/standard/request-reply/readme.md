# JMS Request-Reply Example

To run the example, simply type **mvn verify -Pexample** from this directory.

This example shows you how to handle a request message and receive a reply. To get a reply message, the requesting client creates a temporary queue. Then it sends out the request message with JMSReplyTo set to the temporary queue. The request message is handled by a SimpleRequestServer, who is listening to the request queue for incoming requests. If a request message has arrived, it extracts the reply queue from the request message by JMSReplyTo header, and sends back a reply message. To let the client know to which request message a reply message is related, the broker also set the JMSCorrelationID with the request message's JMSMessageID header to the reply message.

Of course, in a real world example you would re-use the session, producer, consumer and temporary queue and not create a new one for each message! Or better still use the correlation id, and just store the requests in a map, then you don't need a temporary queue at all

Request/Reply style messaging is supported through standard JMS message headers JMSReplyTo and JMSCorrelationID. This is often used in request-reply style communications between applications. Whenever a client sends a message that expects a response, it can use this mechanism to implement. Please consult the JMS 1.1 specification for full details.