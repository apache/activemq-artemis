# Asynchronous Send Acknowledgements Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

Asynchronous Send Acknowledgements are an advanced feature of ActiveMQ Artemis which allow you to receive acknowledgements that messages were successfully received at the broker in a separate thread to the sending thread

In this example we create a normal JMS session, then set a SendAcknowledgementHandler on the JMS session's underlying core session. We send many messages to the broker without blocking and asynchronously receive send acknowledgements via the SendAcknowledgementHandler.

For more information on Asynchronous Send Acknowledgements please see the user manual