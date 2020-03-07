# JMS Expiration Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis so messages are expipired after a certain time.

Messages can be retained in the messaging system for a limited period of time before being removed. JMS specification states that clients should not receive messages that have been expired (but it does not guarantee this will not happen).

ActiveMQ Artemis can assign a _expiry address_ to a given queue so that when messages are expired, they are removed from the queue and routed to this address. These "expired" messages can later be consumed for further inspection.

The example will send 1 message with a short _time-to-live_ to a queue. We will wait for the message to expire and checks that the message is no longer in the queue it was sent to. We will instead consume it from an _expiry queue_ where it was moved when it expired.

## Example setup

Expiry destinations are defined in the configuration file broker.xml.

This configuration will moved expired messages from the `exampleQueue` to the `expiryQueue`.
