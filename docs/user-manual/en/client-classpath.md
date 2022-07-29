# The Client Classpath

Apache ActiveMQ Artemis requires just a single jar on the *client classpath*.

> **Warning**
>
> The client jar mentioned here can be found in the `lib/client` directory of
> the Apache ActiveMQ Artemis distribution.  Be sure you only use the jar from
> the correct version of the release, you *must not* mix and match versions of
> jars from different Apache ActiveMQ Artemis versions. Mixing and matching
> different jar versions may cause subtle errors and failures to occur.


## Maven Packages

The best way to define a client dependency to your java application is through a maven dependency declaration.

There are two packages you can choose from, org.apache.activemq:artemis-jms-client or org.apache.activemq:artemis-jakarta-client for both JMS and Jakarta APIs.

Say you define artemis-version as '{{ config.version }}':

For JMS:
```xml
...
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-jms-client</artifactId>
   <version>${artemis-version}</version>
</dependency>
...
```

For Jakarta:
```xml
...
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-jakarta-client</artifactId>
   <version>${artemis-version}</version>
</dependency>
...
```

## All clients

Even though it is highly recommend using maven, in case this is not a possibility the all inclusive jars could be used. 

These jars will be available under ./lib/client on the main distribution:

- artemis-jakarta-client-all-{{ config.version }}.jar
- artemis-jms-client-all-{{ config.version }}.jar

Whether you are using JMS or just the Core API simply add the
`artemis-jms-client-all.jar` from the `lib/client` directory to your client
classpath. 


**Warning:**These jars will include all the [client's dependencies](client-classpath-jms.md). Be careful with mixing other jars in your application as they may clash with other.


## Individual dependencies

You may also choose to use the jars individually as they are all included under ./lib on the main distribution.

For more information:

- [client jms dependencies](client-classpath-jms.md )
- [client jakarta dependencies](client-classpath-jakarta.md)
