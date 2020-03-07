# Last-Value Queue Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure and use last-value queues.

Last-Value queues are special queues which discard any messages when a newer message with the same value for a well-defined _Last-Value_ property is put in the queue. In other words, a Last-Value queue only retains the last value.

A typical example for Last-Value queue is for stock prices, where you are only interested by the latest value for a particular stock.

The example will send 3 messages with the same _Last-Value_ property to a to a Last-Value queue.

We will browse the queue and see that only the last message is in the queue, the first two messages have been discarded.

We will then consume from the queue the _last_ message.