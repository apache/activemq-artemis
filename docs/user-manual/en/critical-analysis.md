# Critical Analysis of the broker

There are a few things that can go wrong on a production environment:

- Bugs, for more than we try they still happen! We always try to correct them, but that's the only constant in software development.
- IO Errors, disks and hardware can go bad
- Memory issues, the CPU can go crazy by another process

For cases like this, we added a protection to the broker to shut itself down when bad things happen.

This is a feature I hope you won't need it, think it as a safeguard:

We measure time response in places like:

- Queue delivery (add to the queue)
- Journal storage
- Paging operations

If the response time goes beyond a configured timeout, the broker is considered unstable and an action will be taken to either shutdown the broker or halt the VM.

You can use these following configuration options on broker.xml to configure how the critical analysis is performed.


Name | Description
:--- | :---
critical-analyzer | Enable or disable the critical analysis (default true)
critical-analyzer-timeout | Timeout used to do the critical analysis (default 120000 milliseconds)
critical-analyzer-check-period | Time used to check the response times (default half of critical-analyzer-timeout)
critical-analyzer-halt | Should the VM be halted upon failures (default false)

The default for critical-analyzer-halt is false, however the generated broker.xml will have it set to true. That is because we cannot halt the VM if you are embedding ActiveMQ Artemis into an application server or on a multi tenant environment.

The broker on the distribution will then have it set to true, but if you use it in any other way the default will be false.

## What would you expect

- You will see some logs

If you have critical-analyzer-halt=true

```
[Artemis Critical Analyzer] 18:10:00,831 ERROR [org.apache.activemq.artemis.core.server] AMQ224079: The process for the virtual machine will be killed, as component org.apache.activemq.artemis.tests.integration.critical.CriticalSimpleTest$2@5af97850 is not responsive
```

Or if you have critical-analyzer-halt=false

```
[Artemis Critical Analyzer] 18:07:53,475 ERROR [org.apache.activemq.artemis.core.server] AMQ224080: The server process will now be stopped, as component org.apache.activemq.artemis.tests.integration.critical.CriticalSimpleTest$2@5af97850 is not responsive
```

You will see a simple thread dump of the server

```
[Artemis Critical Analyzer] 18:10:00,836 WARN  [org.apache.activemq.artemis.core.server] AMQ222199: Thread dump: AMQ119001: Generating thread dump
*******************************************************************************
===============================================================================
AMQ119002: Thread Thread[Thread-1 (ActiveMQ-scheduled-threads),5,main] name = Thread-1 (ActiveMQ-scheduled-threads) id = 19 group = java.lang.ThreadGroup[name=main,maxpri=10]

sun.misc.Unsafe.park(Native Method)
java.util.concurrent.locks.LockSupport.park(LockSupport.java:175)
java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2039)
java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:1088)
java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue.take(ScheduledThreadPoolExecutor.java:809)
java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:1067)
java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1127)
java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
java.lang.Thread.run(Thread.java:745)
===============================================================================


..... blablablablaba ..........


===============================================================================
AMQ119003: End Thread dump
*******************************************************************************

```

- The Server will be halted if configured to halt

- The system will be stopped if no halt is used:
* Notice that if the system is not behaving well, there is no guarantees the stop will work.


