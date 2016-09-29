# CDI Integration

Apache ActiveMQ Artemis provides a simple CDI integration.  It can either use an embedded broker or connect to a remote broker.

## Configuring a connection

Configuration is provided by implementing the `ArtemisClientConfiguration` interface.

```java
public interface ArtemisClientConfiguration {
   String getHost();

   Integer getPort();

   String getUsername();

   String getPassword();

   String getUrl();

   String getConnectorFactory();

   boolean startEmbeddedBroker();

   boolean isHa();

   boolean hasAuthentication();
}
```

There's a default configuration out of the box, if none is specified.  This will generate an embedded broker.

