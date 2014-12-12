# Logging

ActiveMQ uses the JBoss Logging framework to do its logging and is
configurable via the `logging.properties` file found in the
configuration directories. This is configured by Default to log to both
the console and to a file.

There are 6 loggers available which are as follows:

  Logger                                      Logger Description
  ------------------------------------------- ----------------------------------------------------
  org.jboss.logging                           Logs any calls not handled by the ActiveMQ loggers
  org.apache.activemq.core.server             Logs the core server
  org.apache.activemq.utils                   Logs utility calls
  org.apache.activemq.journal                 Logs Journal calls
  org.apache.activemq.jms                     Logs JMS calls
  org.apache.activemq.integration.bootstrap   Logs bootstrap calls

  : Global Configuration Properties

## Logging in a client or with an Embedded server

Firstly, if you want to enable logging on the client side you need to
include the JBoss logging jars in your library. If you are using maven
add the following dependencies.

    <dependency>
       <groupId>org.jboss.logmanager</groupId>
       <artifactId>jboss-logmanager</artifactId>
       <version>1.3.1.Final</version>
    </dependency>
    <dependency>
       <groupId>org.apache.activemq</groupId>
       <artifactId>activemq-core-client</artifactId>
       <version>2.3.0.Final</version>
    </dependency>

There are 2 properties you need to set when starting your java program,
the first is to set the Log Manager to use the JBoss Log Manager, this
is done by setting the `-Djava.util.logging.manager` property i.e.
`-Djava.util.logging.manager=org.jboss.logmanager.LogManager`

The second is to set the location of the logging.properties file to use,
this is done via the `-Dlogging.configuration` for instance
`-Dlogging.configuration=file:///home/user/projects/myProject/logging.properties`.

> **Note**
>
> The value for this needs to be valid URL

The following is a typical `logging.properties for a client`

    # Root logger option
    loggers=org.jboss.logging,org.apache.activemq.core.server,org.apache.activemq.utils,org.apache.activemq.journal,org.apache.activemq.jms,org.apache.activemq.ra

    # Root logger level
    logger.level=INFO
    # ActiveMQ logger levels
    logger.org.apache.activemq.core.server.level=INFO
    logger.org.apache.activemq.utils.level=INFO
    logger.org.apache.activemq.jms.level=DEBUG

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

## Logging With The JBoss Application Server

When ActiveMQ is deployed within the JBoss Application Server version
7.x or above then it will still use JBoss Logging, refer to the AS7
documentation on how to configure AS7 logging.
