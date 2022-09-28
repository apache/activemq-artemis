# Logging

Apache ActiveMQ Artemis uses the [SLF4J](https://www.slf4j.org/) logging facade for logging,
with the broker assembly providing [Log4J 2](https://logging.apache.org/log4j/2.x/manual/)
as the logging implementation. This is configurable via the `log4j2.properties` file
found in the broker instance `etc` directory, which is configured by default to log to
both the console and to a file.

There are a handful of general loggers available:

Logger | Description
---|---
rootLogger|Logs any calls not handled by the Apache ActiveMQ Artemis loggers
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
logging from the logger `org.foo`.

Then you need to configure the logging level for the `org.foo` logger to `TRACE`,
e.g.:

```
logger.my_logger_ref.name=org.foo
logger.my_logger_ref.level=TRACE
```

## Configuration Reload

Log4J2 has its own configuration file reloading mechanism, which is itself configured via
the same log4j2.properties configuration file. To enable reload upon configuration updates,
set the `monitorInterval` config property to the interval in seconds that the file should
be monitored for updates, e.g:

```
# Monitor config file every 5 seconds for updates
monitorInterval = 5
```

## Logging in a client application

Firstly, if you want to enable logging on the client side you need to
include a logging implementation in your application which supports the
the SLF4J facade. Taking Log4J2 as an example logging implementation,
since it used by the broker, when using Maven your client and logging
dependencies might be e.g.:

```xml
<dependency>
   <groupId>org.apache.activemq</groupId>
   <artifactId>artemis-jms-client</artifactId>
   <version>@PROJECT_VERSION_FILTER_TOKEN@</version>
</dependency>
<dependency>
   <groupId>org.apache.logging.log4j</groupId>
   <artifactId>log4j-slf4j-impl</artifactId>
   <version>2.19.0</version>
</dependency>
```

The Log4J2 configuration can then be supplied via file called `log4j2.properties`
on the classpath which will then be picked up automatically.

Alternatively, use of a specific configuration file can be configured via system
property `log4j2.configurationFile`, e.g.:
```
-Dlog4j2.configurationFile=file:///path/to/custom-log4j2-config.properties
```

The following is an example `log4j2.properties` for a client application,
logging at INFO level to the console and a daily rolling file.

```
# Log4J 2 configuration

# Monitor config file every X seconds for updates
monitorInterval = 5

rootLogger = INFO, console, log_file

logger.activemq.name=org.apache.activemq
logger.activemq.level=INFO

# Console appender
appender.console.type=Console
appender.console.name=console
appender.console.layout.type=PatternLayout
appender.console.layout.pattern=%d %-5level [%logger] %msg%n

# Log file appender
appender.log_file.type = RollingFile
appender.log_file.name = log_file
appender.log_file.fileName = log/application.log
appender.log_file.filePattern = log/application.log.%d{yyyy-MM-dd}
appender.log_file.layout.type = PatternLayout
appender.log_file.layout.pattern = %d %-5level [%logger] %msg%n
appender.log_file.policies.type = Policies
appender.log_file.policies.cron.type = CronTriggeringPolicy
appender.log_file.policies.cron.schedule = 0 0 0 * * ?
appender.log_file.policies.cron.evaluateOnStartup = true
```

## Configuring Broker Audit Logging

There are 3 audit loggers that can be enabled separately and audit 
different types of broker events, these are:

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

These three audit loggers are disabled by default in the broker
`log4j2.properties` configuration file:

```
...
# Audit loggers: to enable change levels from OFF to INFO
logger.audit_base = OFF, audit_log_file
logger.audit_base.name = org.apache.activemq.audit.base
logger.audit_base.additivity = false

logger.audit_resource = OFF, audit_log_file
logger.audit_resource.name = org.apache.activemq.audit.resource
logger.audit_resource.additivity = false

logger.audit_message = OFF, audit_log_file
logger.audit_message.name = org.apache.activemq.audit.message
logger.audit_message.additivity = false
...
```

To *enable* the audit log change the level to `INFO`, like
this:
```
logger.audit_base = INFO, audit_log_file
...
logger.audit_resource = INFO, audit_log_file
...
logger.audit_message = INFO, audit_log_file
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


## More on Log4J2 configuration:

For more detail on configuring Log4J 2, see its [manual](https://logging.apache.org/log4j/2.x/manual/).
