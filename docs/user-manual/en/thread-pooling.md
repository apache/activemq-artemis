# Thread management

This chapter describes how Apache ActiveMQ Artemis uses and pools threads and how you
can manage them.

First we'll discuss how threads are managed and used on the server side,
then we'll look at the client side.

## Server-Side Thread Management

Each Apache ActiveMQ Artemis Server maintains a single thread pool for general use, and
a scheduled thread pool for scheduled use. A Java scheduled thread pool
cannot be configured to use a standard thread pool, otherwise we could
use a single thread pool for both scheduled and non scheduled activity.

Apache ActiveMQ Artemis will, by default, cap its thread pool
at three times the number of cores (or hyper-threads) as reported by `
            Runtime.getRuntime().availableProcessors()` for processing
incoming packets. To override this value, you can set the number of
threads by specifying the parameter `nioRemotingThreads` in the
transport configuration. See the [configuring transports](configuring-transports.md)
for more information on this.

There are also a small number of other places where threads are used
directly, we'll discuss each in turn.

### Server Scheduled Thread Pool

The server scheduled thread pool is used for most activities on the
server side that require running periodically or with delays. It maps
internally to a `java.util.concurrent.ScheduledThreadPoolExecutor`
instance.

The maximum number of thread used by this pool is configure in
`broker.xml` with the `scheduled-thread-pool-max-size`
parameter. The default value is `5` threads. A small number of threads
is usually sufficient for this pool.

### General Purpose Server Thread Pool

This general purpose thread pool is used for most asynchronous actions
on the server side. It maps internally to a
`java.util.concurrent.ThreadPoolExecutor` instance.

The maximum number of thread used by this pool is configure in
`broker.xml` with the `thread-pool-max-size` parameter.

If a value of `-1` is used this signifies that the thread pool has no
upper bound and new threads will be created on demand if there are not
enough threads available to satisfy a request. If activity later
subsides then threads are timed-out and closed.

If a value of `n` where `n`is a positive integer greater than zero is
used this signifies that the thread pool is bounded. If more requests
come in and there are no free threads in the pool and the pool is full
then requests will block until a thread becomes available. It is
recommended that a bounded thread pool is used with caution since it can
lead to dead-lock situations if the upper bound is chosen to be too low.

The default value for `thread-pool-max-size` is `30`.

See the [J2SE
javadoc](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html)
for more information on unbounded (cached), and bounded (fixed) thread
pools.

### Expiry Reaper Thread

A single thread is also used on the server side to scan for expired
messages in queues. We cannot use either of the thread pools for this
since this thread needs to run at its own configurable priority.

For more information on configuring the reaper, please see [message expiry](message-expiry.md).

### Asynchronous IO

Asynchronous IO has a thread pool for receiving and dispatching events
out of the native layer. You will find it on a thread dump with the
prefix ActiveMQ-AIO-poller-pool. Apache ActiveMQ Artemis uses one thread per opened
file on the journal (there is usually one).

There is also a single thread used to invoke writes on libaio. We do
that to avoid context switching on libaio that would cause performance
issues. You will find this thread on a thread dump with the prefix
ActiveMQ-AIO-writer-pool.

## Client-Side Thread Management

On the client side, Apache ActiveMQ Artemis maintains a single, "global"
static scheduled thread pool and a single, "global" static general thread
pool for use by all clients using the same classloader in that JVM instance.

The static scheduled thread pool has a maximum size of `5` threads by
default.  This can be changed using the `scheduledThreadPoolMaxSize` URI
parameter.

The general purpose thread pool has an unbounded maximum size. This is
changed using the `threadPoolMaxSize` URL parameter.

If required Apache ActiveMQ Artemis can also be configured so that each
`ClientSessionFactory` instance does not use these "global" static pools but
instead maintains its own scheduled and general purpose pool. Any
sessions created from that `ClientSessionFactory` will use those pools
instead. This is configured using the `useGlobalPools` boolean URL parameter.