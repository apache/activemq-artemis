# Metrics

Apache ActiveMQ Artemis can export metrics to a variety of monitoring systems
via the [Micrometer](https://micrometer.io/) vendor-neutral application metrics
facade.

Important runtime metrics have been instrumented via the Micrometer API, and
all a user needs to do is implement `org.apache.activemq.artemis.core.server.metrics.ActiveMQMetricsPlugin`
in order to instantiate and configure a `io.micrometer.core.instrument.MeterRegistry`
implementation. Relevant implementations of `MeterRegistry` are available from
the [Micrometer code-base](https://github.com/micrometer-metrics/micrometer/tree/master/implementations).

This is a simple interface:

```java
public interface ActiveMQMetricsPlugin extends Serializable {

   ActiveMQMetricsPlugin init(Map<String, String> options);

   MeterRegistry getRegistry();
}
```

When the broker starts it will call `init` and pass in the `options` which can
be specified in XML as key/value properties. At this point the plugin should
instantiate and configure the `io.micrometer.core.instrument.MeterRegistry`
implementation.

Later during the broker startup process it will call `getRegistry` in order to
get the `MeterRegistry` implementation and use it for registering meters.

The broker ships with two `ActiveMQMetricsPlugin` implementations:

- `org.apache.activemq.artemis.core.server.metrics.plugins.LoggingMetricsPlugin`
  This plugin simply logs metrics. It's not very useful for production, but can
  serve as a demonstration of the Micrometer integration. It takes no key/value
  properties for configuration.

- `org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin`
  This plugin is used for testing. It is in-memory only and provides no external
  output. It takes no key/value properties for configuration.

## Metrics

The following metrics are exported, categorized by component. A description for
each metric is exported along with the metric itself therefore the description
will not be repeated here.

**Broker**

- connection.count
- total.connection.count
- address.memory.usage

**Address**

- routed.message.count
- unrouted.message.count

**Queue**

- message.count
- durable.message.count
- persistent.size
- durable.persistent.size
- delivering.message.count
- delivering.durable.message.count
- delivering.persistent.size
- delivering.durable.persistent.size
- scheduled.message.count
- scheduled.durable.message.count
- scheduled.persistent.size
- scheduled.durable.persistent.size
- messages.acknowledged
- messages.added
- messages.killed
- messages.expired
- consumer.count

It may appear that some higher level broker metrics are missing (e.g. total
message count). However, these metrics can be deduced by aggregating the
lower level metrics (e.g. aggregate the message.count metrics from all queues
to get the total).

JVM memory metrics are also exported by default and GC and thread metrics can
be configured

## Configuration

Metrics for all addresses and queues are enabled by default. If you want to
disable metrics for a particular address or set of addresses you can do so by
setting the `enable-metrics` `address-setting` to `false`.

In `broker.xml` use the `metrics` element to configure which JVM metrics are
reported and to configure the plugin itself. Here's a configuration with all
JVM metrics:

```xml
<metrics>
   <jvm-memory>true</jvm-memory> <!-- defaults to true -->
   <jvm-gc>true</jvm-gc> <!-- defaults to false -->
   <jvm-threads>true</jvm-threads> <!-- defaults to false -->
   <plugin class-name="org.apache.activemq.artemis.core.server.metrics.plugins.LoggingMetricsPlugin"/>
</metrics>
```

The plugin can also be configured with key/value properties in order to
customize the implementation as necessary, e.g.:

```xml
<metrics>
   <plugin class-name="org.example.MyMetricsPlugin">
      <property key="host" value="example.org" />
      <property key="port" value="5162" />
      <property key="foo" value="10" />
   </plugin>
</metrics>
```