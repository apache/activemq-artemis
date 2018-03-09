# Versions

This chapter provides the information for each release:
- A link to the full release notes which includes all issues resolved in the release.
- A brief list of "highlights."
- If necessary, specific steps required when upgrading from the previous version. 
  - **Note:** If the upgrade spans multiple versions then the steps from **each** version need to be followed in order.
  - **Note:** Follow the general upgrade procedure outlined in the [Upgrading the Broker](upgrading.md) 
    chapter in addition to any version-specific upgrade instructions outlined here. 

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
