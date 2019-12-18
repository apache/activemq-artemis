# Logging

Apache ActiveMQ Artemis uses the JBoss Logging framework to do its logging and is
configurable via the `logging.properties` file found in the
configuration directories. This is configured by Default to log to both
the console and to a file.

There are 8 loggers available which are as follows:

Logger | Description
---|---
org.jboss.logging|Logs any calls not handled by the Apache ActiveMQ Artemis loggers
org.apache.activemq.artemis.core.server|Logs the core server
org.apache.activemq.artemis.utils|Logs utility calls
org.apache.activemq.artemis.journal|Logs Journal calls
org.apache.activemq.artemis.jms|Logs JMS calls
org.apache.activemq.artemis.integration.bootstrap|Logs bootstrap calls
org.apache.activemq.audit.base|audit log. Disabled by default
org.apache.activemq.audit.message|message audit log. Disabled by default


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

## Configuring Audit Log

The 2 audit loggers can be enabled to record some important operations like
create/delete queues. By default this logger is disabled. The configuration
(logging.properties) for audit log is like this by default:

```$xslt
logger.org.apache.activemq.audit.base.level=ERROR
logger.org.apache.activemq.audit.base.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.base.useParentHandlers=false

logger.org.apache.activemq.audit.message.level=ERROR
logger.org.apache.activemq.audit.message.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.message.useParentHandlers=false
...
```

To enable the audit log change the above level to INFO, like this:
```$xslt
logger.org.apache.activemq.audit.base.level=INFO
logger.org.apache.activemq.audit.base.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.base.useParentHandlers=false

logger.org.apache.activemq.audit.message.level=INFO
logger.org.apache.activemq.audit.message.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.message.useParentHandlers=false
...
```

The 2 audit loggers can be disable/enable separately. The second logger
(org.apache.activemq.audit.message) audits messages in 'hot path'
(code path that is very sensitive to performance, e.g. sending messages).
Turn on this audit logger may affect the performance.

Once enabled, all audit records are written into a separate log
file (by default audit.log).

## Use Custom Handlers

To use a different handler than the built-in ones, you either pick one from
existing libraries or you implement it yourself. All handlers must extends the
java.util.logging.Handler class.

To enable a custom handler you need to append it to the handlers list
`logger.handlers` and add its configuration to the `logging.configuration`.

Last but not least, once you get your own handler please [add it to the boot
classpath](using-server.md#adding-bootstrap-dependencies) otherwise the log
manager will fail to load it!
