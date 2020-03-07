# Apache ActiveMQ Artemis Plugin Support

Apache ActiveMQ Artemis is designed to allow extra functionality to be added by
creating a plugin. Multiple plugins can be registered at the same time and they
will be chained together and executed in the order they are registered (i.e.
the first plugin registered is always executed first).

Creating a plugin is very simple. It requires:

- Implementing the [`ActiveMQServerPlugin`](https://github.com/apache/activemq-artemis/blob/master/artemis-server/src/main/java/org/apache/activemq/artemis/core/server/plugin/ActiveMQServerPlugin.java)
  interface
- Making sure the plugin is [on the classpath](using-server.md#adding-runtime-dependencies)
- Registering it with the broker either via [xml](#registering-a-plugin) or [programmatically](#registering-a-plugin-programmatically).

Only the methods that you want to add behavior for need to be implemented as
all of the interface methods are default methods.

## Registering a Plugin

To register a plugin with by XML you need to add the `broker-plugins` element
at the `broker.xml`. It is also possible to pass configuration to a plugin
using the `property` child element(s). These properties (zero to many) will be
read and passed into the plugin's `init(Map<String, String>)` operation after
the plugin has been instantiated.

```xml
<broker-plugins>
   <broker-plugin class-name="some.plugin.UserPlugin">
      <property key="property1" value="val_1" />
      <property key="property2" value="val_2" />
   </broker-plugin>
</broker-plugins>
```

## Registering a Plugin Programmatically

For registering a plugin programmatically you need to call the
`registerBrokerPlugin()` method and pass in a new instance of your plugin.  In
the example below assuming your plugin is called `UserPlugin`, registering it
looks like the following:


``` java
...

Configuration config = new ConfigurationImpl();
...

config.registerBrokerPlugin(new UserPlugin());
```

## Using the `LoggingActiveMQServerPlugin`

The `LoggingActiveMQServerPlugin` logs specific broker events.

You can select which events are logged by setting the following configuration
properties to `true`.

Property|Trigger Event|Default Value
---|---|---
`LOG_CONNECTION_EVENTS`|Connection is created/destroy.|`false`
`LOG_SESSION_EVENTS`|Session is created/closed.|`false`
`LOG_CONSUMER_EVENTS`|Consumer is created/closed|`false`
`LOG_DELIVERING_EVENTS`|Message is delivered to a consumer and when a message is acknowledged by a consumer.|`false`
`LOG_SENDING_EVENTS`|When a message has been sent to an address and when a message has been routed within the broker.|`false`
`LOG_INTERNAL_EVENTS`|When a queue created/destroyed, when a message is expired, when a bridge is deployed and when a critical failure occurs.|`false`
`LOG_ALL_EVENTS`|Includes all the above events.|`false`

By default the `LoggingActiveMQServerPlugin` will not log any information. The
logging is activated by setting one (or a selection) of the above configuration
properties to `true`.

To configure the plugin, you can add the following configuration to the broker.
In the example below both `LOG_DELIVERING_EVENTS` and `LOG_SENDING_EVENTS` will
be logged by the broker.

```xml
<broker-plugins>
   <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin">
      <property key="LOG_DELIVERING_EVENTS" value="true" />
      <property key="LOG_SENDING_EVENTS" value="true" />
   </broker-plugin>
</broker-plugins>
```

Most events in the `LoggingActiveMQServerPlugin` follow a `beforeX` and
`afterX` notification pattern (e.g `beforeCreateConsumer()` and
`afterCreateConsumer()`).

At Log Level `INFO`, the LoggingActiveMQServerPlugin logs an entry when an
`afterX` notification occurs. By setting the logger
`org.apache.activemq.artemis.core.server.plugin.impl` to `DEBUG`, log entries
are generated for both `beforeX` and `afterX` notifications. Log level `DEBUG`
will also log more information for a notification when available.

## Using the NotificationActiveMQServerPlugin

The NotificationActiveMQServerPlugin can be configured to send extra
notifications for specific broker events.

You can select which notifications are sent by setting the following
configuration properties to `true`.

Property|Property Description|Default Value
---|---
`SEND_CONNECTION_NOTIFICATIONS`|Sends a notification when a Connection is created/destroy.|`false`.
`SEND_SESSION_NOTIFICATIONS`|Sends a notification when a Session is created/closed.|`false`.
`SEND_ADDRESS_NOTIFICATIONS`|Sends a notification when an Address is added/removed.|`false`.
`SEND_DELIVERED_NOTIFICATIONS`|Sends a notification when message is delivered to a consumer.|`false`
`SEND_EXPIRED_NOTIFICATIONS`|Sends a notification when message has been expired by the broker.|`false`

By default the NotificationActiveMQServerPlugin will not send any
notifications. The plugin is activated by setting one (or a selection) of the
above configuration properties to `true`.

To configure the plugin, you can add the following configuration to the broker.
In the example below both `SEND_CONNECTION_NOTIFICATIONS` and
`SEND_SESSION_NOTIFICATIONS` will be sent by the broker.

```xml
<broker-plugins>
   <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin">
      <property key="SEND_CONNECTION_NOTIFICATIONS" value="true" />
      <property key="SEND_SESSION_NOTIFICATIONS" value="true" />
   </broker-plugin>
</broker-plugins>
```

