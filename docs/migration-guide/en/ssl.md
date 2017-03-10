SSL
=====================================

The next interesting security related topic is encrypting transport layer using SSL. Both ActiveMQ and Artemis leverage JDK's Java Secure Socket Extension (JSSE), so things should be easy to migrate.
  
Let's recap quickly how SSL is used in ActiveMQ. First, you need to define the *SSL Context*. You can do that using `<sslContext>` configuration section in `conf/activemq.xml`, like

```xml
<sslContext>
    <sslContext keyStore="file:${activemq.conf}/broker.ks" keyStorePassword="password"/>
</sslContext>  
```

The SSL context defines key and trust stores to be used by the broker. After this, you set your transport connector with the `ssl` schema and  preferably some additional options. 

```xml
<transportConnectors>
    <transportConnector name="ssl" uri="ssl://localhost:61617?transport.needClientAuth=true"/>
</transportConnectors>
```

These options are related to [SSLServerSocket](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLServerSocket.html) and are specified as URL parameters with the `transport.` prefix, like `needClientAuth` shown in the example above.

In Artemis, Netty is responsible for all things related to the transport layer, so it handles SSL for us as well. All configuration options are set directly on the acceptor, like

```xml
<acceptors>
    <acceptor name="netty-ssl-acceptor">tcp://localhost:61617?sslEnabled=true;keyStorePath=${data.dir}/../etc/broker.ks;keyStorePassword=password;needClientAuth=true</acceptor>
</acceptors>
```

Note that we used the same Netty connector schema and just added `sslEnabled=true` parameter to use it with SSL. Next, we can go ahead and define key and trust stores. There's a slight difference in parameter naming between two brokers, as shown in the table below. 

| ActiveMQ           | Artemis                   |
| --                 | --                        |
| keyStore           | keyStorePath              |
| keyStorePassword   | keyStorePassword          |
| trustStore         | trustStorePath            |
| trustStorePassword | trustStorePassword        |

Finally, you can go and set all other `SSLServerSocket` parameters you need (like `needClientAuth` in this example). There's no extra prefix needed for this in Artemis. 

It's important to note that you should be able to reuse your existing key and trust stores and just copy them to the new broker.




