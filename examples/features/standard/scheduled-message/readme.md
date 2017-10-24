# JMS Scheduled Message Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to send a scheduled message to a JMS Queue using ActiveMQ Artemis.

A Scheduled Message is a message that will be delivered at a time specified by the sender. To do this, simply set a `org.apache.activemq.artemis.api.core.Message.HDR_SCHEDULED_DELIVERY_TIME` header property. The value of the property should be the time of delivery in milliseconds.

In this example, a message is created with the scheduled delivery time set to 5 seconds after the current time.