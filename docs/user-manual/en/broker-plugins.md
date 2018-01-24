# Apache ActiveMQ Artemis Plugin Support

Apache ActiveMQ Artemis is designed to allow extra functionality to be added by
creating a plugin. Multiple plugins can be registered at the same time and they will be chained
together and executed in the order they are registered.  (i.e. the first plugin registered 
is always executed first).

Creating a plugin is very simple. It requires implementing the [`ActiveMQServerPlugin`](https://github.com/apache/activemq-artemis/blob/master/artemis-server/src/main/java/org/apache/activemq/artemis/core/server/plugin/ActiveMQServerPlugin.java)
interface, making sure the plugin is on the classpath, and registering it with the broker.  Only the methods that you want to add behavior for need to be implemented as all of the interface methods are default methods.

## Adding the plugin to the classpath

See the documentation on [adding runtime dependencies](using-server.md) to understand how to make your plugin available to the broker.

If you are using an embed system than you will need the jar under the regular classpath of your embedded application.

## Registering a Plugin

To register a plugin with by XML you need to add the `broker-plugins` element at the `broker.xml`. It is also possible
to pass configuration to a plugin using the `property` child element(s). These properties (zero to many)
will be read and passed into the Plugin's `init(Map<String, String>)` operation after the plugin
has been instantiated.

```xml
<configuration ...>

...
    <broker-plugins>
        <broker-plugin class-name="some.plugin.UserPlugin">
            <property key="property1" value="val_1" />
            <property key="property2" value="val_2" />
        </broker-plugin>
    </broker-plugins>
...

</configuration>
```

## Registering a Plugin Programmatically

For registering a plugin programmatically you need to call the
registerBrokerPlugin() method and pass in a new instance of your plugin.  In the example below
assuming your plugin is called `UserPlugin`, registering it looks like the following:


``` java

...

Configuration config = new ConfigurationImpl();
...

config.registerBrokerPlugin(new UserPlugin());
```

## Using the LoggingActiveMQServerPlugin

The LoggingActiveMQServerPlugin logs specific broker events.

You can select which events are logged by setting the following configuration properties to `true`.

<table summary="LoggingActiveMQServerPlugin configuration" border="1">
    <colgroup>
        <col/>
        <col/>
    </colgroup>
    <thead>
    <tr>
        <th>Property</th>
        <th>Property Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>LOG_CONNECTION_EVENTS</td>
        <td>Log info when a Connection is created/destroy. Default `false`.</td>
    </tr>
    <tr>
        <td>LOG_SESSION_EVENTS</td>
        <td>Log info when a Session is created/closed. Default `false`.</td>
    </tr>
    <tr>
        <td>LOG_CONSUMER_EVENTS</td>
        <td>Logs info when a Consumer is created/closed. Default `false`.</td>
    </tr>
    <tr>
        <td>LOG_DELIVERING_EVENTS</td>
        <td>Logs info when message is delivered to a consumer and when a message is acknowledged by a consumer.
        Default `false`</td>
    </tr>
    <tr>
        <td>LOG_SENDING_EVENTS</td>
        <td>Logs info when a message has been sent to an address and when a message has been routed within the broker.
         Default `false`</td>
    </tr>
    <tr>
        <td>LOG_INTERNAL_EVENTS</td>
        <td>Logs info when a queue created/destroyed, when a message is expired, when a bridge is deployed and when a critical
        failure occurs. Default `false`</td>
    </tr>
    <tr>
         <td>LOG_ALL_EVENTS</td>
         <td>Logs info for all the above events. Default `false`</td>
        </tr>
    </tbody>
</table>

By default the LoggingActiveMQServerPlugin wil not log any information. The logging is activated by setting one (or a selection)
of the above configuration properties to `true`.

To configure the plugin, you can add the following configuration to the broker. In the example below both LOG_DELIVERING_EVENTS
and LOG_SENDING_EVENTS will be logged by the broker.

```xml
<configuration ...>

...
    <broker-plugins>
        <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin">
            <property key="LOG_DELIVERING_EVENTS" value="true" />
            <property key="LOG_SENDING_EVENTS" value="true" />
        </broker-plugin>
    </broker-plugins>
...

</configuration>
```

Most events in the LoggingActiveMQServerPlugin follow a `beforeX` and `afterX` notification pattern e.g beforeCreateConsumer() and afterCreateConsumer().

At Log Level `INFO`, the LoggingActiveMQServerPlugin logs an entry when an `afterX` notification occurs. By setting the Logger
"org.apache.activemq.artemis.core.server.plugin.impl" to `DEBUG` Level, log entries are generated for both `beforeX` and `afterX` notifications.
Log Level `DEBUG` will also log more information for a notification when available.
