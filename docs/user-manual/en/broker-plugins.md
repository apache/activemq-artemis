# Apache ActiveMQ Artemis Plugin Support

Apache ActiveMQ Artemis is designed to allow extra functionality to be added by
creating a plugin. Multiple plugins can be registered at the same time and they will be chained
together and executed in the order they are registered.  (i.e. the first plugin registered 
is always executed first).

Creating a plugin is very simple. It requires implementing the [`ActiveMQServerPlugin`](../../../artemis-server/src/main/java/org/apache/activemq/artemis/core/server/plugin/ActiveMQServerPlugin.java)
interface, making sure the plugin is on the classpath, and registering it with the broker.  Only the methods that you want to add behavior for need to be implemented as all of the interface methods are default methods.

## Adding the plugin to the classpath

The proper place to add your jar would be under $ARTEMIS_INSTANCE/lib.

If you are using an embed system than you will need the jar under the regular classpath of your embedded application.

## Registering a Plugin

To register a plugin with by XML you need to add the `broker-plugins` element at the `broker.xml`.

```xml
<configuration ...>

...
    <broker-plugins>
        <broker-plugin class-name="some.plugin.UserPlugin" />
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


