# Logging

Apache ActiveMQ Artemis uses the JBoss Logging framework to do its logging and is
configurable via the `logging.properties` file found in the
configuration directories. This is configured by Default to log to both
the console and to a file.

There are 6 loggers available which are as follows:

<table summary="Loggers" border="1">
    <colgroup>
        <col/>
        <col/>
    </colgroup>
    <thead>
    <tr>
        <th>Logger</th>
        <th>Logger Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>org.jboss.logging</td>
        <td>Logs any calls not handled by the Apache ActiveMQ Artemis loggers</td>
    </tr>
    <tr>
        <td>org.apache.activemq.artemis.core.server</td>
        <td>Logs the core server</td>
    </tr>
    <tr>
        <td>org.apache.activemq.artemis.utils</td>
        <td>Logs utility calls</td>
    </tr>
    <tr>
        <td>org.apache.activemq.artemis.journal</td>
        <td>Logs Journal calls</td>
    </tr>
    <tr>
        <td>org.apache.activemq.artemis.jms</td>
        <td>Logs JMS calls</td>
    </tr>
    <tr>
        <td>org.apache.activemq.artemis.integration.bootstrap </td>
        <td>Logs bootstrap calls</td>
    </tr>
    </tbody>
</table>

  : Global Configuration Properties

## Logging in a client or with an Embedded server

Firstly, if you want to enable logging on the client side you need to
include the JBoss logging jars in your library. If you are using maven
add the following dependencies.

    <dependency>
       <groupId>org.jboss.logmanager</groupId>
       <artifactId>jboss-logmanager</artifactId>
       <version>1.5.3.Final</version>
    </dependency>
    <dependency>
       <groupId>org.apache.activemq</groupId>
       <artifactId>activemq-core-client</artifactId>
       <version>1.0.0.Final</version>
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
