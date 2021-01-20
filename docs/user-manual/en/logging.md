# Logging

Apache ActiveMQ Artemis uses the JBoss Logging framework to do its logging and is
configurable via the `logging.properties` file found in the `etc` directory. This
is configured by default to log to both the console and to a file.

There are a handful of general loggers available:

Logger | Description
---|---
org.jboss.logging|Logs any calls not handled by the Apache ActiveMQ Artemis loggers
org.apache.activemq.artemis.core.server|Logs the core server
org.apache.activemq.artemis.utils|Logs utility calls
org.apache.activemq.artemis.journal|Logs Journal calls
org.apache.activemq.artemis.jms|Logs JMS calls
org.apache.activemq.artemis.integration.bootstrap|Logs bootstrap calls
org.apache.activemq.audit.base|audit log. Disabled by default
org.apache.activemq.audit.resource|resource audit log. Disabled by default
org.apache.activemq.audit.message|message audit log. Disabled by default

## Activating TRACE for a specific logger

Sometimes it is necessary to get more detailed logs from a particular logger. For
example, when you're trying to troublshoot an issue. Say you needed to get TRACE
logging from the logger `org.foo`. First you would need to add `org.foo` to the
`loggers` list at the top of `logging.properties`, e.g.:

```
loggers=...,org.foo
```

Then you need to configure the logging level for the `org.foo` logger to `TRACE`,
e.g.:

```
logger.org.foo.level=TRACE
```

Lastly, you would need to update the `level` of the necessary `handler`
to allow the `TRACE` logging through, e.g.:

```
handler.CONSOLE.level=TRACE
```
or
```
handler.FILE.level=TRACE
```

## Logging in a client or with an Embedded server

Firstly, if you want to enable logging on the client side you need to
include the JBoss logging jars in your application. If you are using 
Maven the simplest way is to use the "all" client jar, e.g.:

```xml
<dependency>
   <groupId>org.jboss.logmanager</groupId>
   <artifactId>jboss-logmanager</artifactId>
   <version>2.1.10.Final</version>
</dependency>
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>activemq-core-client-all</artifactId>
   <version>2.16.0</version>
</dependency>
```

There are 2 properties you need to set when starting your java program,
the first is to set the Log Manager to use the JBoss Log Manager, this
is done by setting the `-Djava.util.logging.manager` property i.e.:
```
-Djava.util.logging.manager=org.jboss.logmanager.LogManager
```

The second is to set the location of the logging.properties file to use,
this is done via the `-Dlogging.configuration`, e.g.:
```
-Dlogging.configuration=file:///home/user/projects/myProject/logging.properties
```

> **Note:**
>
> The `logging.configuration` system property needs to be valid URL

The following is a typical `logging.properties` for a client

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

## Configuring Audit Logging

There are 3 audit loggers that can be enabled separately and audit 
different types of events, these are:

1. **base**: This is a highly verbose logger that will capture most 
   events that occur on JMX beans.
2. **resource**: This logs the creation of, updates to, and deletion
   of resources such as addresses and queues as well as authentication.
   The main purpose of this is to track console activity and access
   to the broker.
3. **message**: This logs the production and consumption of messages.

> **Note:**
>
> All extra logging will negatively impact performance. Whether or not
> the performance impact is "too much" will depend on your use-case.

These three audit loggers are disabled by default in the 
`logging.properties` configuration file:

```
loggers=...,org.apache.activemq.audit.base,org.apache.activemq.audit.message,org.apache.activemq.audit.resource
...
logger.org.apache.activemq.audit.base.level=ERROR
logger.org.apache.activemq.audit.base.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.base.useParentHandlers=false

logger.org.apache.activemq.audit.resource.level=ERROR
logger.org.apache.activemq.audit.resource.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.resource.useParentHandlers=false

logger.org.apache.activemq.audit.message.level=ERROR
logger.org.apache.activemq.audit.message.handlers=AUDIT_FILE
logger.org.apache.activemq.audit.message.useParentHandlers=false
...
```

To *enable* the audit log change the `level` attributes to `INFO`, like
this:
```
logger.org.apache.activemq.audit.base.level=INFO
...
logger.org.apache.activemq.audit.resource.level=INFO
...
logger.org.apache.activemq.audit.message.level=INFO
```

The 3 audit loggers can be disable/enabled separately. 

Once enabled, all audit records are written into a separate log
file (by default audit.log).

### Logging the clients remote address

It is possible to configure the audit loggers to log the remote address of any calling clients either through normal 
clients or through the console and JMX. This is configured by enabling a specific login module in the login config file. 
```
org.apache.activemq.artemis.spi.core.security.jaas.AuditLoginModule optional
       debug=false;
```


> **Note:**
>
> This needs to be the first entry in the login.config file

> **Note:**
>
> This login module does no authentication, it is used only to catch client information through which ever path a client takes


## Use Custom Handlers

To use a different handler than the built-in ones, you either pick one from
existing libraries or you implement it yourself. All handlers must extends the
`java.util.logging.Handler` class.

To enable a custom handler you need to append it to the handlers list
`logger.handlers` and add its configuration to the `logging.configuration`.

Last but not least, once you get your own handler please [add it to the boot
classpath](using-server.md#adding-bootstrap-dependencies) otherwise the log
manager will fail to load it!
