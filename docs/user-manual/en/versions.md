# Versions

This chapter provides the following information for each release:
- A link to the full release notes which includes all issues resolved in the release.
- A brief list of "highlights" when applicable.
- If necessary, specific steps required when upgrading from the previous version. 
  - **Note:** If the upgrade spans multiple versions then the steps from **each** version need to be followed in order.
  - **Note:** Follow the general upgrade procedure outlined in the [Upgrading the Broker](upgrading.md) 
    chapter in addition to any version-specific upgrade instructions outlined here.

## 2.17.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12349326).

Highlights:
- [Message-level authorization](broker-plugins.md#using-the-brokermessageauthorizationplugin) similar to ActiveMQ 5.x.
- A count of addresses and queues is now available from the management API.
- You can now reload the broker's configuration from disk via the management API rather than waiting for the periodic 
  disk scan to pick it up
- Performance improvements on libaio journal.
- New command-line option to transfer messages.
- Performance improvements for the wildcard address manager.
- JDBC datasource property values can now be masked.
- Lots of usability improvements to the Hawtio 2 based web console introduced in 2.16.0
- New management method to create a core bridge using JSON-based configuration input.

## 2.16.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12348718).

Highlights:
- Configurable namespace for temporary queues
- [AMQP Server Connectivity](amqp-broker-connections.md)
- "Basic" [`SecurityManager` implementation](security.md#basic-security-manager) that supports replication
- Consumer window size support for individual STOMP clients
- Improved JDBC connection management
- New web console based on Hawtio 2
- Performance optimizations (i.e. caching) for authentication and authorization
- Support for admin objects in the JCA resource adapter to facilitate deployment into 3rd-party Java EE application servers
- Ability to prevent an acceptor from automatically starting

#### Upgrading from older versions

Due to [ARTEMIS-2893](https://issues.apache.org/jira/browse/ARTEMIS-2893) the
fundamental way user management was implemented had to change to avoid data
integrity issues related to concurrent modification. From a user's perspective
two main things changed:

1. User management is no longer possible using the `artemis user` commands
   when the broker is **offline**. Of course users are still free to modify the
   properties files directly in this situation.
2. The parameters of the `artemis user` commands changed. Instead of using
   something like this:
   ```sh
   ./artemis user add --user guest --password guest --role admin
   ``` 
   Use this instead:
   ```sh
   ./artemis user add --user-command-user guest --user-command-password guest --role admin
   ```
   In short, use `user-command-user` in lieu of `user` and `user-command-password`
   in lieu of `password`. Both `user` and `password` parameters now apply to the
   connection used to send the command to the broker.
   
   For additional details see [ARTEMIS-2893](https://issues.apache.org/jira/browse/ARTEMIS-2893)
   and [ARTEMIS-3010](https://issues.apache.org/jira/browse/ARTEMIS-3010) 

## 2.15.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12348568).

Highlights:
- Ability to use FQQN syntax for both `security-settings` and JNDI lookup
- Support pausing dispatch during group rebalance (to avoid potential out-of-order consumption)
- Socks5h support

## 2.14.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12348290).

Highlights:
- Management methods to update diverts
- Ability to "disable" a queue so that messages are not routed to it
- Support JVM GC & thread metrics
- Support for resetting queue properties by unsetting them in `broker.xml`
- Undeploy diverts by removing them from `broker.xml`
- Add `addressMemoryUsagePercentage` and `addressSize` as metrics

#### Upgrading from older versions

This is likely a rare situation, but it's worth mentioning here anyway. Prior
to 2.14.0 if you configured a parameter on a `queue` in `broker.xml` (e.g.
`max-consumers`) and then later *removed* that setting the configured value you
set would remain. This has changed in 2.14.0 via ARTEMIS-2797. Any value that
is not explicitly set in `broker.xml` will be set back to either the static
default or the dynamic default configured in the address-settings (e.g. via
`default-max-consumers` in this example). Therefore, ensure any existing queues
have all the needed parameters set in `broker.xml` values before upgrading.

## 2.13.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12348088).

Highlights:
- Management methods for an address' duplicate ID cache to check the cache's size and clear it
- Support for [min/max expiry-delay](message-expiry.md#configuring-expiry-delay)
- [Per-acceptor security domains](security.md#per-acceptor-security-domains)
- Command-line `check` tool for checking the health of a broker
- Support disabling metrics per address via the [`enable-metrics` address setting](address-model.md#configuring-addresses-and-queues-via-address-settings)
- Improvements to the [audit logging](logging.md#configuring-audit-logging)
- Speed optimizations for the `HierarchicalObjectRepository`, an internal object used to store address and security settings

#### Upgrading from older versions

Version 2.13.0 added new [audit logging](logging.md#configuring-audit-logging)
which is logged at `INFO` level and can be very verbose. The
`logging.properties` shipped with this new version is set up to filter this out
by default. If your `logging.properties` isn't updated appropriately this audit
logging will likely appear in your console and `artemis.log` file assuming
you're using a logging configuration close to the default. Add this to your
`logging.properties`:

```
# to enable audit change the level to INFO
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

#Audit logger
handler.AUDIT_FILE=org.jboss.logmanager.handlers.PeriodicRotatingFileHandler
handler.AUDIT_FILE.level=INFO
handler.AUDIT_FILE.properties=suffix,append,autoFlush,fileName
handler.AUDIT_FILE.suffix=.yyyy-MM-dd
handler.AUDIT_FILE.append=true
handler.AUDIT_FILE.autoFlush=true
handler.AUDIT_FILE.fileName=${artemis.instance}/log/audit.log
handler.AUDIT_FILE.formatter=AUDIT_PATTERN

formatter.AUDIT_PATTERN=org.jboss.logmanager.formatters.PatternFormatter
formatter.AUDIT_PATTERN.properties=pattern
formatter.AUDIT_PATTERN.pattern=%d [AUDIT](%t) %s%E%n
```

## 2.12.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12346675).

Highlights:
- Support for [SOCKS proxy](configuring-transports.md#configuring-netty-socks-proxy)
- Real [large message](large-messages.md) support for AMQP
- [Automatic creation of dead-letter resources](undelivered-messages.md#automatically-creating-dead-letter-resources) akin to ActiveMQ 5's individual dead-letter strategy
- [Automatic creation of expiry resources](message-expiry.md#configuring-automatic-creation-of-expiry-resources)
- Improved API for queue creation
- Allow users to override JAVA_ARGS via environment variable
- Reduce heap usage during journal loading during broker start-up
- Allow `server` header in STOMP `CONNECTED` frame to be disabled
- Support disk store used percentage as an exportable metric (e.g. to be monitored by tools like Prometheus, etc.)
- Ability to configure a "[customizer](https://www.eclipse.org/jetty/javadoc/9.4.26.v20200117/org/eclipse/jetty/server/HttpConfiguration.Customizer.html)" for the embedded web server
- Improved logging for errors when starting an `acceptor` to more easily identify the `acceptor` which has the problem.
- The CLI will now read the `broker.xml` to find the default `connector` URL for commands which require it (e.g. `consumer`, `producer`, etc.)

## 2.11.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12346258).

Highlights:
- Support [retroactive addresses](retroactive-addresses.md).
- Support downstream federated [queues](federation-queue.md#configuring-downstream-federation) and [addresses](federation-address.md#configuring-downstream-federation).
- Make security manager [configurable via XML](security.md#custom-security-manager).
- Support pluggable SSL [TrustManagerFactory](configuring-transports.md#configuring-netty-ssl).
- Add plugin support for federated queues/addresses.
- Support `com.sun.jndi.ldap.read.timeout` in [LDAPLoginModule](security.md#ldaploginmodule).

## 2.10.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12345602).

This was mainly a bug-fix release with a notable dependency change impacting version upgrade.

#### Upgrading from 2.9.0

Due to the WildFly dependency upgrade the broker start scripts/configuration need to be adjusted after upgrading.

##### On \*nix

Locate this statement in `bin/artemis`:
```
WILDFLY_COMMON="$ARTEMIS_HOME/lib/wildfly-common-1.5.1.Final.jar"
```
This needs to be replaced with this:
```
WILDFLY_COMMON="$ARTEMIS_HOME/lib/wildfly-common-1.5.2.Final.jar"
```

##### On Windows

Locate this part of `JAVA_ARGS` in `etc/artemis.profile.cmd` respectively `bin/artemis-service.xml`:
```
%ARTEMIS_HOME%\lib\wildfly-common-1.5.1.Final.jar
```
This needs to be replaced with this:
```
%ARTEMIS_HOME%\lib\wildfly-common-1.5.2.Final.jar
```

## 2.9.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12345527).

This was a light release. It included a handful of bug fixes, a few improvements, and one major new feature.

Highlights:
- Support [exporting metrics](metrics.md).

## 2.8.1

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12345432).

This was mainly a bug-fix release with a notable dependency change impacting version upgrade.

#### Upgrading from 2.8.0

Due to the dependency upgrade made on [ARTEMIS-2319](https://issues.apache.org/jira/browse/ARTEMIS-2319) the
broker start scripts need to be adjusted after upgrading.

##### On \*nix

Locate this `if` statement in `bin/artemis`:
```
if [ -z "$LOG_MANAGER" ] ; then
 # this is the one found when the server was created
 LOG_MANAGER="$ARTEMIS_HOME/lib/jboss-logmanager-2.0.3.Final.jar"
fi
```
This needs to be replaced with this block:
```
if [ -z "$LOG_MANAGER" ] ; then
 # this is the one found when the server was created
 LOG_MANAGER="$ARTEMIS_HOME/lib/jboss-logmanager-2.1.10.Final.jar"
fi

WILDFLY_COMMON=`ls $ARTEMIS_HOME/lib/wildfly-common*jar 2>/dev/null`
if [ -z "$WILDFLY_COMMON" ] ; then
 # this is the one found when the server was created
 WILDFLY_COMMON="$ARTEMIS_HOME/lib/wildfly-common-1.5.1.Final.jar"
fi
```
Notice that the `jboss-logmanager` version has changed and there is also a new `wildfly-common` library.

Not much further down there is this line:
```
-Xbootclasspath/a:"$LOG_MANAGER" \
```
This line should be changed to be:
```
-Xbootclasspath/a:"$LOG_MANAGER:$WILDFLY_COMMON" \
```

##### On Windows

Locate this part of `JAVA_ARGS` in `etc/artemis.profile.cmd` respectively `bin/artemis-service.xml`:
```
-Xbootclasspath/a:%ARTEMIS_HOME%\lib\jboss-logmanager-2.1.10.Final.jar
```
This needs to be replaced with this:
```
-Xbootclasspath/a:%ARTEMIS_HOME%\lib\jboss-logmanager-2.1.10.Final.jar;%ARTEMIS_HOME%\lib\wildfly-common-1.5.1.Final.jar
```

## 2.8.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12345169).

Highlights:
- Support ActiveMQ5 feature [JMSXGroupFirstForConsumer](message-grouping.md#notifying-consumer-of-group-ownership-change).
- Clarify handshake timeout error with remote address.
- Support [duplicate detection](duplicate-detection.md) for AMQP messages the same as core.

## 2.7.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12342977).

Highlights:
- Support advanced destination options like `consumersBeforeDispatchStarts` and `timeBeforeDispatchStarts` from 5.x.
- Add support for delays before deleting addresses and queues via [`auto-delete-queues-delay` and `auto-delete-addresses-delay`
  Address Settings](address-model.md#configuring-addresses-and-queues-via-address-settings).
- Support [logging HTTP access](web-server.md).
- Add a CLI command to purge a queue.
- Support user and role manipulation for PropertiesLoginModule via management interfaces.
- [Docker images](https://github.com/apache/activemq-artemis/tree/master/artemis-docker).
- [Audit logging](logging.md#configuring-audit-log).
- Implementing [consumer priority](consumer-priority).
- Support [FQQN](address-model.md#specifying-a-fully-qualified-queue-name) for producers.
- Track routed and unrouted messages sent to an address.
- Support [connection pooling in LDAPLoginModule](security.md#ldaploginmodule).
- Support configuring a default consumer window size via [`default-consumer-window-size` Address Setting](address-model.md#configuring-addresses-and-queues-via-address-settings).
- Support [masking](masking-passwords.md) `key-store-password` and `trust-store-password` in management.xml.
- Support [`JMSXGroupSeq` -1 to close/reset message groups](message-grouping.md#closing-a-message-group) from 5.x.
- Allow configuration of [RMI registry port](management.md#configuring-remote-jmx-access).
- Support routing-type configuration on [core bridge](core-bridges.md#configuring-bridges).
- Move artemis-native as its own project, as [activemq-artemis-native](https://github.com/apache/activemq-artemis-native).
- Support [federated queues and addresses](federation.md).

## 2.6.4

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12344010).

This was mainly a bug-fix release with a few improvements a couple notable new features:

Highlights:
- Added the ability to set the text message content on the `producer` CLI command.
- Support reload logging configuration at runtime.

## 2.6.3

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12343472).

This was mainly a bug-fix release with a few improvements but no substantial new features.

## 2.6.2

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12343404).

This was a bug-fix release with no substantial new features or improvements.

## 2.6.1

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12343356).

This was a bug-fix release with no substantial new features or improvements.

## 2.6.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12342903).

Highlights:
- Support [regular expressions for matching client certificates](security.md#certificateloginmodule).
- Support `SASL_EXTERNAL` for AMQP clients.
- New examples showing [virtual topic mapping](examples.md#openwire) and [exclusive queue](examples.md#exclusive-queue) features.

## 2.5.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12342127).

Highlights:
- [Exclusive consumers](exclusive-queues.md).
- Equivalent ActiveMQ 5.x Virtual Topic naming abilities.
- SSL Certificate revocation list.
- [Last-value queue](last-value-queues.md) support for OpenWire.
- Support [masked passwords](masking-passwords.md) in bootstrap.xm and login.config
- Configurable [broker plugin](broker-plugins.md#using-the-loggingactivemqserverplugin) implementation for logging various broker events (i.e. `LoggingActiveMQServerPlugin`).
- Option to use OpenSSL provider for Netty via the [`sslProvider`](configuring-transports.md#configuring-netty-ssl) URL parameter.
- Enable [splitting of broker.xml into multiple files](configuration-index.md).
- Enhanced message count and size metrics for queues.

#### Upgrading from 2.4.0

1. Due to changes from [ARTEMIS-1644](https://issues.apache.org/jira/browse/ARTEMIS-1644) any `acceptor` that needs to be
   compatible with HornetQ and/or Artemis 1.x clients needs to have `anycastPrefix=jms.queue.;multicastPrefix=jms.topic.`
   in the `acceptor` url. This prefix used to be configured automatically behind the scenes when the broker detected 
   these old types of clients, but that broke certain use-cases with no possible work-around. See 
   [ARTEMIS-1644](https://issues.apache.org/jira/browse/ARTEMIS-1644) for more details.

## 2.4.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12341540).

Highlights:
- [JMX configuration via XML](management.md#role-based-authorisation-for-jmx) rather than having to use system properties via command line or start script.
- Configuration of [max frame payload length for STOMP web-socket](protocols-interoperability.md#stomp-over-web-sockets).
- Ability to configure HA using JDBC persistence.
- Implement [role-based access control for management objects](management.md).

#### Upgrading from 2.3.0

1. Create `<ARTEMIS_INSTANCE>/etc/management.xml`. At the very least, the file must contain this:
   ```xml
   <management-context xmlns="http://activemq.org/schema"/>
   ```
   This configures role based authorisation for JMX. Read more in the [Management](management.md) documentation.
1. If configured, remove the Jolokia war file from the `web` element in `<ARTEMIS_INSTANCE>/etc/bootstrap.xml`:
   ```xml
   <app url="jolokia" war="jolokia.war"/>
   ``` 
   This is no longer required as the Jolokia REST interface is now integrated into the console web application.
   
   If the following is absent and you desire to deploy the web console then add:
   ```xml
   <app url="console" war="console.war"/>
   ```   
   **Note:** the Jolokia REST interface URL will now be at `http://<host>:<port>/console/jolokia`


## 2.3.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12341247).

Highlights:
- [Web admin console](management-console.md)!
- [Critical Analysis](critical-analysis.md) and deadlock detection on broker
- Support [Netty native kqueue](configuring-transports.md#macos-native-transport) on Mac.
- [Last-value queue](last-value-queues.md) for AMQP

#### Upgrading from 2.2.0

1. If you desire to deploy the web console then add the following to the `web` element in `<ARTEMIS_INSTANCE>/etc/bootstrap.xml`:
   ```xml
   <app url="console" war="console.war"/>
   ```


## 2.2.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12340541).

Highlights:
- Scheduled messages with the STOMP protocol.
- Support for JNDIReferenceFactory and JNDIStorable.
- Ability to delete queues and addresses when [broker.xml changes](config-reload.md).
- [Client authentication via Kerberos TLS Cipher Suites (RFC 2712)](security.md#kerberos-authentication).


## 2.1.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12339963).

Highlights:
- [Broker plugin support](broker-plugins.md).
- Support [Netty native epoll](configuring-transports.md#linux-native-transport) on Linux.
- Ability to configure arbitrary security role mappings.
- AMQP performance improvements.


## 2.0.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12338813).

Highlights:
- Huge update involving a significant refactoring of the [addressing model](address-model.md) yielding the following benefits:
  - Simpler and more flexible XML configuration.
  - Support for additional messaging use-cases.
  - Eliminates confusing JMS-specific queue naming conventions (i.e. "jms.queue." & "jms.topic." prefixes).
- Pure encoding of messages so protocols like AMQP don't need to convert messages to "core" format unless absolutely necessary.
- ["MAPPED" journal type](persistence.md#memory-mapped) for increased performance in certain use-cases.


## 1.5.6

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12340547).

Highlights:
- Bug fixes.


## 1.5.5

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12339947).

Highlights:
- Bug fixes.


## 1.5.4

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12339158).

Highlights:
- Support Oracle12C for JDBC persistence.
- Bug fixes.


## 1.5.3

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12339575).

Highlights:
- Support "byte notation" (e.g. "K", "KB", "Gb", etc.) in broker XML configuration.
- CLI command to recalculate disk sync times.
- Bug fixes.


## 1.5.2

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12338833).

Highlights:
- Support for paging using JDBC.
- Bug fixes.


## 1.5.1

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12338661).

Highlights:
- Support outgoing connections for AMQP.
- Bug fixes.


## 1.5.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12338118).

Highlights:
- AMQP performance improvements.
- JUnit rule implementation so messaging resources like brokers can be easily configured in tests.
- Basic CDI integration.
- Store user's password in hash form by default.


## 1.4.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12336052).

Highlights:
- "Global" limit for disk usage.
- Detect and reload certain XML configuration changes at runtime.
- MQTT interceptors.
- Support adding/deleting queues via CLI.
- New "browse" security permission for clients who only wish to look at messages.
- Option to populate JMSXUserID.
- "Dual authentication" support to authenticate SSL-based and non-SSL-based clients differently.


## 1.3.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12328978).

Highlights:
- Better support of OpenWire features (e.g. reconnect, producer flow-control, optimized acknowledgements)
- SSL keystore reload at runtime.
- Initial support for JDBC persistence.
- Support scheduled messages on last-value queue.

## 1.2.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12333274).

Highlights:
- Improvements around performance
- OSGi support.
- Support functionality equivalent to all 5.x JAAS login modules including:
  - Properties file
  - LDAP
  - SSL certificate
  - "Guest"


## 1.1.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12332642&projectId=12315920).

Highlights:
- MQTT support.
- The examples now use the CLI programmatically to create, start, stop, etc. servers reflecting real cases used in 
  production.
- CLI improvements. There are new tools to compact the journal and additional improvements to the user experience.
- Configurable resource limits.
- Ability to disable server-side message load-balancing.


## 1.0.0

[Full release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12315920&version=12328953).

Highlights:
- First release of the [donated code-base](http://mail-archives.apache.org/mod_mbox/activemq-dev/201407.mbox/%3cCAKF+bsovr7Hvn-rMYkb3pF6hoGjx7nuJWzT_Nh8MyC4usRBX9A@mail.gmail.com%3e) as ActiveMQ Artemis!
- Lots of features for parity with ActiveMQ 5.x including:
  - OpenWire support
  - AMQP 1.0 support
  - URL based connections
  - Auto-create addresses/queues
  - Jolokia integration
