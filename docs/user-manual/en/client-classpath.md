# The Client Classpath

## Maven dependencies

The recommended way to define a client dependency for your java application is through a Maven dependency declaration.

There are two dependencies you can choose from, `org.apache.activemq:artemis-jms-client` for JMS 2.0 or `org.apache.activemq:artemis-jakarta-client` for Jakarta Messaging 3.x.

For JMS:
```xml
...
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-jms-client</artifactId>
   <version>@PROJECT_VERSION_FILTER_TOKEN@</version>
</dependency>
...
```

For Jakarta:
```xml
...
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-jakarta-client</artifactId>
   <version>@PROJECT_VERSION_FILTER_TOKEN@</version>
</dependency>
...
```

## Individual client dependencies

If you dont wish to use a build tool such as Maven which manages the dependencies for you, you may also choose to add the specific dependency jars to your classpath, which are all included under ./lib on the main distribution.

For more information of the clients individual dependencies, see:
- [JMS client dependencies](client-classpath-jms.md )
- [Jakarta client dependencies](client-classpath-jakarta.md)

## Repackaged '-all' clients

Even though it is highly recommend to use the maven dependencies, in cases this isnt a possibility and neither is using the individual dependencies as detailed above then the all-inclusive repackaged jar could be used as an alternative.

These jars are available at Maven Central:
- [artemis-jms-client-all-{{ config.version }}.jar](https://repo.maven.apache.org/maven2/org/apache/activemq/artemis-jms-client-all/{{ config.version }}/)
- [artemis-jakarta-client-all-{{ config.version }}.jar](https://repo.maven.apache.org/maven2/org/apache/activemq/artemis-jakarta-client-all/{{ config.version }}/)

Whether you are using JMS or just the Core API simply add the `artemis-jms-client-all` jar to your client classpath.
For Jakarta Messaging add the `artemis-jakarta-client-all` jar instead.

**Warning:**These repackaged jars include all the [client's dependencies](client-classpath-jms.md). Be careful with mixing other components jars in your application as they may clash with each other.



