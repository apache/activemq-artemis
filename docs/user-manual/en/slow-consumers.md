#Detecting Slow Consumers

In this section we will discuss how Apache ActiveMQ Artemis can be configured to deal
with slow consumers. A slow consumer with a server-side queue (e.g. JMS
topic subscriber) can pose a significant problem for broker performance.
If messages build up in the consumer's server-side queue then memory
will begin filling up and the broker may enter paging mode which would
impact performance negatively. However, criteria can be set so that
consumers which don't acknowledge messages quickly enough can
potentially be disconnected from the broker which in the case of a
non-durable JMS subscriber would allow the broker to remove the
subscription and all of its messages freeing up valuable server
resources.

## Configuration required for detecting slow consumers

By default the server will not detect slow consumers. If slow consumer
detection is desired then see [queue attributes chapter](queue-attributes.md)
for more details.

The calculation to determine whether or not a consumer is slow inspects two notable
metrics:

1. The queue's message count.

2. The number of messages a consumer has acknowledged.

The queue's message count is inspected to ensure that the queue actually has had enough
messages to actually satisfy the consumer's threshold. For example, it would not be
fair to mark a consumer as "slow" if the queue received no messages. This is also notable
because in order to get an accurate message count the queue must be locked which can
negatively impact performance in high-throughput use-cases. Therefore slow-consumer
detection is only recommended on queues where it is absolutely necessary and in those
cases it may be worth tuning the `slow-consumer-check-period` to ensure it's not
running so often as to negatively impact performance.

Finally, the algorithm inspects the number of messages a particular consumer has
*acknowledged*. It doesn't take into account whether or not flow control
has been enabled on the consumer, whether or not the consumer is
streaming a large message, etc. Keep this in mind when configuring slow
consumer detection.

Please note that slow consumer checks are performed using the scheduled
thread pool and that each queue on the broker with slow consumer
detection enabled will cause a new entry in the internal
`java.util.concurrent.ScheduledThreadPoolExecutor` instance. If there
are a high number of queues and the `slow-consumer-check-period` is
relatively low then there may be delays in executing some of the checks.
However, this will not impact the accuracy of the calculations used by
the detection algorithm. See [thread pooling](thread-pooling.md) for more details about this pool.
