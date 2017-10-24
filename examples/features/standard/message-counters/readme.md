# JMS Message Counter Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to use message counters to obtain message information for a JMS queue.

The example will show how to configure sampling of message counters.

We will produce and consume 1 message from a queue. Interleaved with the JMS operation, we will retrieve the queue's message counters at different times to display the metrics on the queue.

## Example setup

Message counter is configured in the broker configuration file broker.xml:

    <message-counter-enabled>true</message-counter-enabled>
    <message-counter-sample-period>2000</message-counter-sample-period>
    <message-counter-max-day-history>2</message-counter-max-day-history>

By default, message counters are not enabled (for performance reason). To enable them, set `message-counter-enabled` to `true`.
Queues are sampled every 10 seconds by default. For this example we will reduce it to 2 seconds by setting `message-counter-sample-period` to `2000`.
ActiveMQ Artemis holds in memory the message counters' history for a maximum number of days (10 by default). We can change the number of days the history is kept by setting the `message-counter-max-day-history` parameter.

The sample period and the max day history parameters have a small impact on the performance of ActiveMQ Artemis (the resources taken to sample a queue are not available to the system's normal use). You should set these parameters accordingly to the use and throughput of your messages.