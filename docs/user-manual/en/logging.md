# Logging

Apache ActiveMQ Artemis uses the JBoss Logging framework to do its logging and is
configurable via the `logging.properties` file found in the
configuration directories. This is configured by Default to log to both
the console and to a file.

There are 6 loggers available which are as follows:

Logger | Description
---|---
org.jboss.logging|Logs any calls not handled by the Apache ActiveMQ Artemis loggers
org.apache.activemq.artemis.core.server|Logs the core server
org.apache.activemq.artemis.utils|Logs utility calls
org.apache.activemq.artemis.journal|Logs Journal calls
org.apache.activemq.artemis.jms|Logs JMS calls
org.apache.activemq.artemis.integration.bootstrap|Logs bootstrap calls


## Logging in a client or with an Embedded server

Firstly, if you want to enable logging on the client side you need to
include the JBoss logging jars in your library. If you are using Maven
the simplest way is to use the "all" client jar.

```xml
<dependency>
   <groupId>org.jboss.logmanager</groupId>
   <artifactId>jboss-logmanager</artifactId>
   <version>2.0.3.Final</version>
</dependency>
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>activemq-core-client</artifactId>
   <version>2.5.0</version>
</dependency>
```

There are 2 properties you need to set when starting your java program,
the first is to set the Log Manager to use the JBoss Log Manager, this
is done by setting the `-Djava.util.logging.manager` property i.e.
`-Djava.util.logging.manager=org.jboss.logmanager.LogManager`

The second is to set the location of the logging.properties file to use,
this is done via the `-Dlogging.configuration` for instance
`-Dlogging.configuration=file:///home/user/projects/myProject/logging.properties`.

> **Note:**
>
> The `logging.configuration` system property needs to be valid URL

The following is a typical `logging.properties for a client`

```
# Root logger option
loggers=org.jboss.logging,org.apache.activemq.artemis.core.server,org.apache.activemq.artemis.utils,org.apache.activemq.artemis.journal,org.apache.activemq.artemis.jms,org.apache.activemq.artemis.ra

# Root logger level
logger.level=INFO
# Apache ActiveMQ Artemis logger levels
logger.org.apache.activemq.artemis.core.server.level=INFO
logger.org.apache.activemq.artemis.utils.level=INFO
logger.org.apache.activemq.artemis.jms.level=DEBUG

# Root logger handlers
logger.handlers=FILE,CONSOLE

# Console handler configuration
handler.CONSOLE=org.jboss.logmanager.handlers.ConsoleHandler
handler.CONSOLE.properties=autoFlush
handler.CONSOLE.level=FINE
handler.CONSOLE.autoFlush=true
handler.CONSOLE.formatter=PATTERN

# File handler configuration
handler.FILE=org.jboss.logmanager.handlers.FileHandler
handler.FILE.level=FINE
handler.FILE.properties=autoFlush,fileName
handler.FILE.autoFlush=true
handler.FILE.fileName=activemq.log
handler.FILE.formatter=PATTERN

# Formatter pattern configuration
formatter.PATTERN=org.jboss.logmanager.formatters.PatternFormatter
formatter.PATTERN.properties=pattern
formatter.PATTERN.pattern=%d{HH:mm:ss,SSS} %-5p [%c] %s%E%n
```