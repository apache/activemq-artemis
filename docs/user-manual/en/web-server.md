# Embedded Web Server

Apache ActiveMQ Artemis embeds the [Jetty web
server](https://www.eclipse.org/jetty/). Its main purpose is to host the [Management
Console](management-console.md). However, it can also host other web
applications like the [REST interface](rest.md) or even Spring-based web
applications (e.g. using Camel).

## Configuration

The embedded Jetty instance is configured in `etc/bootstrap.xml` via the `web`
element, e.g.:

```xml
<web bind="http://localhost:8161" path="web">
   <app url="activemq-branding" war="activemq-branding.war"/>
   <app url="artemis-plugin" war="artemis-plugin.war"/>
   <app url="console" war="console.war"/>
</web>
```

The `web` element has the following attributes:

- `bind` The protocol to use (i.e. `http` or `https`) as well as the host and
  port on which to listen.
- `path` The name of the subdirectory in which to find the web application
  archives (i.e. WAR files). This is a subdirectory of the broker's home or
  instance directory.
- `customizer` The name of customizer class to load.
- `clientAuth` Whether or not clients should present an SSL certificate when
  they connect. Only applicable when using `https`.
- `passwordCodec` The custom coded to use for unmasking the `keystorePassword`
  and `trustStorePassword`.
- `keyStorePath` The location on disk of the keystore. Only applicable when
  using `https`.
- `keyStorePassword` The password to the keystore. Only applicable when using
  `https`. Can be masked using `ENC()` syntax or by defining `passwordCodec`.
  See more in the [password masking](masking-passwords.md) chapter.
- `trustStorePath` The location on disk for the truststore. Only applicable when
  using `https`.
- `trustStorePassword` The password to the truststore. Only applicable when
  using `https`. Can be masked using `ENC()` syntax or by defining
  `passwordCodec`. See more in the [password masking](masking-passwords.md)
  chapter.
- `includedTLSProtocols` A comma seperated list of included TLS protocols,
  ie `"TLSv1,TLSv1.1,TLSv1.2"`. Only applicable when using `https`.
- `excludedTLSProtocols` A comma seperated list of excluded TLS protocols,
  ie `"TLSv1,TLSv1.1,TLSv1.2"`. Only applicable when using `https`.
- `includedCipherSuites` A comma seperated list of included cipher suites.
  Only applicable when using `https`.
- `excludedCipherSuites` A comma seperated list of excluded cipher suites.
  Only applicable when using `https`.

Each web application should be defined in an `app` element. The `app` element
has the following attributes:

- `url` The context to use for the web application.
- `war` The name of the web application archive on disk.

It's also possible to configure HTTP/S request logging via the `request-log`
element which has the following attributes:

- `filename` The full path of the request log. This attribute is required.
- `append` Whether or not to append to the existing log or truncate it. Boolean flag.
- `extended` Whether or not to use the extended request log format. Boolean flag.
- `logCookies` Logging of the request cookies. Boolean flag.
- `logTimeZone` The output file name of the request log.
- `filenameDateFormat` The log file name date format.
- `retainDays` The number of days before rotated log files are deleted.
- `ignorePaths` Request paths that will not be logged. Comma delimited list.
- `logDateFormat` The timestamp format string for request log entries.
- `logLocale` The locale of the request log.
- `logLatency` Logging of request processing time. Boolean flag.
- `logServer` Logging of the request hostname. Boolean flag.
- `preferProxiedForAddress` Whether the actual IP address of the connection or
  the IP address from the `X-Forwarded-For` header will be logged. Boolean flag.

These attributes are essentially passed straight through to the underlying
[`org.eclipse.jetty.server.NCSARequestLog`](https://www.eclipse.org/jetty/javadoc/current/org/eclipse/jetty/server/NCSARequestLog.html)
instance. Default values are based on this implementation.

Here is an example configuration:

```xml
<web bind="http://localhost:8161" path="web">
   <app url="activemq-branding" war="activemq-branding.war"/>
   <app url="artemis-plugin" war="artemis-plugin.war"/>
   <app url="console" war="console.war"/>
   <request-log filename="${artemis.instance}/log/http-access-yyyy_MM_dd.log" append="true" extended="true"/>
</web>
```

## Proxy Forwarding

The proxies and load balancers usually support `X-Forwarded` headers
to send information altered or lost when a proxy is involved
in the path of the request. Jetty supports the [`ForwardedRequestCustomizer`](https://www.eclipse.org/jetty/javadoc/current/org/eclipse/jetty/server/ForwardedRequestCustomizer.html)
customizer to handle `X-Forwarded` headers.
Set the `customizer` attribute via the `web` element to enable the [`ForwardedRequestCustomizer`](https://www.eclipse.org/jetty/javadoc/current/org/eclipse/jetty/server/ForwardedRequestCustomizer.html) customizer, ie:

```xml
<web bind="http://localhost:8161" path="web" customizer="org.eclipse.jetty.server.ForwardedRequestCustomizer">
   <app url="activemq-branding" war="activemq-branding.war"/>
   <app url="artemis-plugin" war="artemis-plugin.war"/>
   <app url="console" war="console.war"/>
</web>
```