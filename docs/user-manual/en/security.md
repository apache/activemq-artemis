# Security

This chapter describes how security works with Apache ActiveMQ Artemis and how
you can configure it.

To disable security completely simply set the `security-enabled` property to
`false` in the `broker.xml` file.

For performance reasons both **authentication and authorization is cached**
independently. Entries are removed from the caches (i.e. invalidated) either
when the cache reaches its maximum size in which case the least-recently used
entry is removed or when an entry has been in the cache "too long".

The size of the caches are controlled by the `authentication-cache-size` and
`authorization-cache-size` configuration parameters. Both default to `1000`.

How long cache entries are valid is controlled by
`security-invalidation-interval`, which is in milliseconds. Using `0` will
disable caching. The default is `10000` ms.

## Tracking the Validated User

To assist in security auditing the `populate-validated-user` option exists. If
this is `true` then the server will add the name of the validated user to the
message using the key `_AMQ_VALIDATED_USER`.  For JMS and Stomp clients this is
mapped to the key `JMSXUserID`. For users authenticated based on their SSL
certificate this name is the name to which their certificate's DN maps. If
`security-enabled` is `false` and `populate-validated-user` is `true` then the
server will simply use whatever user name (if any) the client provides. This
option is `false` by default.

## Role based security for addresses

Apache ActiveMQ Artemis contains a flexible role-based security model for
applying security to queues, based on their addresses.

As explained in [Using Core](core.md), Apache ActiveMQ Artemis core consists
mainly of sets of queues bound to addresses. A message is sent to an address
and the server looks up the set of queues that are bound to that address, the
server then routes the message to those set of queues.

Apache ActiveMQ Artemis allows sets of permissions to be defined against the
queues based on their address. An exact match on the address can be used or a
[wildcard match](wildcard-syntax.md) can be used.

There are different permissions that can be given to the set of queues which match the
address. Those permissions are:

- `createAddress`. This permission allows the user to create an address fitting
  the `match`.

- `deleteAddress`. This permission allows the user to delete an address fitting
  the `match`.

- `createDurableQueue`. This permission allows the user to create a durable
  queue under matching addresses.

- `deleteDurableQueue`. This permission allows the user to delete a durable
  queue under matching addresses.

- `createNonDurableQueue`. This permission allows the user to create a
  non-durable queue under matching addresses.

- `deleteNonDurableQueue`. This permission allows the user to delete a
  non-durable queue under matching addresses.

- `send`. This permission allows the user to send a message to matching
  addresses.

- `consume`. This permission allows the user to consume a message from a queue
  bound to matching addresses.

- `browse`. This permission allows the user to browse a queue bound to the
  matching address.

- `manage`. This permission allows the user to invoke management operations by
  sending management messages to the management address.

For each permission, a list of roles who are granted that permission is
specified. If the user has any of those roles, he/she will be granted that
permission for that set of addresses.

Let's take a simple example, here's a security block from `broker.xml` file:

```xml
<security-setting match="globalqueues.europe.#">
   <permission type="createDurableQueue" roles="admin"/>
   <permission type="deleteDurableQueue" roles="admin"/>
   <permission type="createNonDurableQueue" roles="admin, guest, europe-users"/>
   <permission type="deleteNonDurableQueue" roles="admin, guest, europe-users"/>
   <permission type="send" roles="admin, europe-users"/>
   <permission type="consume" roles="admin, europe-users"/>
</security-setting>
```    

Using the default [wildcard syntax](wildcard-syntax.md) the `#` character
signifies "any sequence of words". Words are delimited by the `.` character.
Therefore, the above security block applies to any address that starts with the
string "globalqueues.europe.".

Only users who have the `admin` role can create or delete durable queues bound
to an address that starts with the string "globalqueues.europe."

Any users with the roles `admin`, `guest`, or `europe-users` can create or
delete temporary queues bound to an address that starts with the string
"globalqueues.europe."

Any users with the roles `admin` or `europe-users` can send messages to these
addresses or consume messages from queues bound to an address that starts with
the string "globalqueues.europe."

The mapping between a user and what roles they have is handled by the security
manager. Apache ActiveMQ Artemis ships with a user manager that reads user
credentials from a file on disk, and can also plug into JAAS or JBoss
Application Server security.

For more information on configuring the security manager, please see 'Changing
the Security Manager'.

There can be zero or more `security-setting` elements in each xml file.  Where
more than one match applies to a set of addresses the *more specific* match
takes precedence.

Let's look at an example of that, here's another `security-setting` block:

```xml
<security-setting match="globalqueues.europe.orders.#">
   <permission type="send" roles="europe-users"/>
   <permission type="consume" roles="europe-users"/>
</security-setting>
```

In this `security-setting` block the match 'globalqueues.europe.orders.\#' is
more specific than the previous match 'globalqueues.europe.\#'. So any
addresses which match 'globalqueues.europe.orders.\#' will take their security
settings *only* from the latter security-setting block.

Note that settings are not inherited from the former block. All the settings
will be taken from the more specific matching block, so for the address
'globalqueues.europe.orders.plastics' the only permissions that exist are
`send` and `consume` for the role europe-users. The permissions
`createDurableQueue`, `deleteDurableQueue`, `createNonDurableQueue`,
`deleteNonDurableQueue` are not inherited from the other security-setting
block.

By not inheriting permissions, it allows you to effectively deny permissions in
more specific security-setting blocks by simply not specifying them. Otherwise
it would not be possible to deny permissions in sub-groups of addresses.

### Fine-grained security using fully qualified queue name

In certain situations it may be necessary to configure security that is more
fine-grained that simply across an entire address. For example, consider an
address with multiple queues:

```xml
<addresses>
   <address name="foo">
      <anycast>
         <queue name="q1" />
         <queue name="q2" />
      </anycast>
   </address>
</addresses>
```

You may want to limit consumption from `q1` to one role and consumption from
`q2` to another role. You can do this using the fully qualified queue name (i.e.
FQQN) in the `match` of the `security-setting`, e.g.:

```xml
<security-setting match="foo::q1">
   <permission type="consume" roles="q1Role"/>
</security-setting>
<security-setting match="foo::q2">
   <permission type="consume" roles="q2Role"/>
</security-setting>
```
**Note:** Wildcard matching doesn't work in conjuction with FQQN. The explicit
goal of using FQQN here is to be *exact*.

## Security Setting Plugin

Aside from configuring sets of permissions via XML these permissions can
alternatively be configured via a plugin which implements
`org.apache.activemq.artemis.core.server.SecuritySettingPlugin` e.g.:

```xml
<security-settings>
   <security-setting-plugin class-name="org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin">
      <setting name="initialContextFactory" value="com.sun.jndi.ldap.LdapCtxFactory"/>
      <setting name="connectionURL" value="ldap://localhost:1024"/>
      <setting name="connectionUsername" value="uid=admin,ou=system"/>
      <setting name="connectionPassword" value="secret"/>
      <setting name="connectionProtocol" value="s"/>
      <setting name="authentication" value="simple"/>
   </security-setting-plugin>
</security-settings>
```

Most of this configuration is specific to the plugin implementation. However,
there are two configuration details that will be specified for every
implementation:

- `class-name`. This attribute of `security-setting-plugin` indicates the name
  of the class which implements
  `org.apache.activemq.artemis.core.server.SecuritySettingPlugin`.

- `setting`. Each of these elements represents a name/value pair that will be
  passed to the implementation for configuration purposes.

See the JavaDoc on
`org.apache.activemq.artemis.core.server.SecuritySettingPlugin` for further
details about the interface and what each method is expected to do.

### Available plugins

#### LegacyLDAPSecuritySettingPlugin

This plugin will read the security information that was previously handled by
[`LDAPAuthorizationMap`](http://activemq.apache.org/security.html) and the
[`cachedLDAPAuthorizationMap`](http://activemq.apache.org/cached-ldap-authorization-module.html)
in Apache ActiveMQ 5.x and turn it into Artemis security settings where
possible. The security implementations of ActiveMQ 5.x and Artemis don't match
perfectly so some translation must occur to achieve near equivalent
functionality.

Here is an example of the plugin's configuration:

```xml
<security-setting-plugin class-name="org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin">
   <setting name="initialContextFactory" value="com.sun.jndi.ldap.LdapCtxFactory"/>
   <setting name="connectionURL" value="ldap://localhost:1024"/>
   <setting name="connectionUsername" value="uid=admin,ou=system"/>
   <setting name="connectionPassword" value="secret"/>
   <setting name="connectionProtocol" value="s"/>
   <setting name="authentication" value="simple"/>
</security-setting-plugin>
```

- `class-name`. The implementation is
  `org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin`.

- `initialContextFactory`. The initial context factory used to connect to LDAP.
  It must always be set to `com.sun.jndi.ldap.LdapCtxFactory` (i.e. the default
  value).

- `connectionURL`. Specifies the location of the directory server using an ldap
  URL, `ldap://Host:Port`. You can optionally qualify this URL, by adding a
  forward slash, `/`, followed by the DN of a particular node in the directory
  tree. For example, `ldap://ldapserver:10389/ou=system`. The default is
  `ldap://localhost:1024`.

- `connectionUsername`. The DN of the user that opens the connection to the
  directory server. For example, `uid=admin,ou=system`.  Directory servers
  generally require clients to present username/password credentials in order to
  open a connection.

- `connectionPassword`. The password that matches the DN from
  `connectionUsername`. In the directory server, in the DIT, the password is
  normally stored as a `userPassword` attribute in the corresponding directory
  entry.

- `connectionProtocol`. Currently the only supported value is a blank string.
  In future, this option will allow you to select the Secure Socket Layer (SSL)
for the connection to the directory server. **Note:** this option must be set
explicitly to an empty string, because it has no default value.

- `authentication`. Specifies the authentication method used when binding to
  the LDAP server. Can take either of the values, `simple` (username and
password, the default value) or `none` (anonymous). **Note:** Simple Authentication
and Security Layer (SASL) authentication is currently not supported.

- `destinationBase`. Specifies the DN of the node whose children provide the
  permissions for all destinations. In this case the DN is a literal value
  (that is, no string substitution is performed on the property value).  For
  example, a typical value of this property is
  `ou=destinations,o=ActiveMQ,ou=system` (i.e. the default value).

- `filter`. Specifies an LDAP search filter, which is used when looking up the
  permissions for any kind of destination.  The search filter attempts to match
  one of the children or descendants of the queue or topic node. The default
  value is `(cn=*)`.

- `roleAttribute`. Specifies an attribute of the node matched by `filter`,
  whose value is the DN of a role. Default value is `uniqueMember`.

- `adminPermissionValue`. Specifies a value that matches the `admin`
  permission. The default value is `admin`.

- `readPermissionValue`. Specifies a value that matches the `read` permission.
  The default value is `read`.

- `writePermissionValue`. Specifies a value that matches the `write`
  permission. The default value is `write`.

- `enableListener`. Whether or not to enable a listener that will automatically
  receive updates made in the LDAP server and update the broker's authorization
  configuration in real-time. The default value is `true`.

- `mapAdminToManage`. Whether or not to map the legacy `admin` permission to the
  `manage` permission. See details of the mapping semantics below. The default
   value is `false`.

- `allowQueueAdminOnRead`. Whether or not to map the legacy `read` permission to
  the `createDurableQueue`, `createNonDurableQueue`, and `deleteDurableQueue`
  permissions so that JMS clients can create durable and non-durable subscriptions
  without needing the `admin` permission. This was allowed in ActiveMQ 5.x. The
  default value is `false`.

The name of the queue or topic defined in LDAP will serve as the "match" for
the security-setting, the permission value will be mapped from the ActiveMQ 5.x
type to the Artemis type, and the role will be mapped as-is.

ActiveMQ 5.x only has 3 permission types - `read`, `write`, and `admin`. These
permission types are described on their
[website](http://activemq.apache.org/security.html). However, as described
previously, ActiveMQ Artemis has 9 permission types - `createAddress`,
`deleteAddress`, `createDurableQueue`, `deleteDurableQueue`,
`createNonDurableQueue`, `deleteNonDurableQueue`, `send`, `consume`, `browse`,
and `manage`. Here's how the old types are mapped to the new types:

- `read` - `consume`, `browse`
- `write` - `send`
- `admin` - `createAddress`, `deleteAddress`, `createDurableQueue`,
  `deleteDurableQueue`, `createNonDurableQueue`, `deleteNonDurableQueue`,
  `manage` (if `mapAdminToManage` is `true`)

As mentioned, there are a few places where a translation was performed to
achieve some equivalence.:

- This mapping doesn't include the Artemis `manage` permission type by default
  since there is no type analogous for that in ActiveMQ 5.x. However, if
  `mapAdminToManage` is `true` then the legacy `admin` permission will be
  mapped to the `manage` permission.

- The `admin` permission in ActiveMQ 5.x relates to whether or not the broker
  will auto-create a destination if it doesn't exist and the user sends a
  message to it. Artemis automatically allows the automatic creation of a
  destination if the user has permission to send message to it. Therefore, the
  plugin will map the `admin` permission to the 6 aforementioned permissions in
  Artemis by default. If `mapAdminToManage` is `true` then the legacy `admin`
  permission will be mapped to the `manage` permission as well.

## Secure Sockets Layer (SSL) Transport

When messaging clients are connected to servers, or servers are connected to
other servers (e.g. via bridges) over an untrusted network then Apache ActiveMQ
Artemis allows that traffic to be encrypted using the Secure Sockets Layer
(SSL) transport.

For more information on configuring the SSL transport, please see [Configuring
the Transport](configuring-transports.md).

## User credentials

Apache ActiveMQ Artemis ships with three security manager implementations:

- The flexible, pluggable `ActiveMQJAASSecurityManager` which supports any
  standard JAAS login module. Artemis ships with several login modules which
  will be discussed further down. This is the default security manager.

- The `ActiveMQBasicSecurityManager` which doesn't use JAAS and only supports
  auth via username & password credentials. It also supports adding, removing,
  and updating users via the management API. All user & role data is stored
  in the broker's bindings journal which means any changes made to a live
  broker will be available on its backup.

- The legacy, deprecated `ActiveMQSecurityManagerImpl` that reads user
  credentials, i.e. user names, passwords and role information from properties
  files on the classpath called `artemis-users.properties` and
  `artemis-roles.properties`.

### JAAS Security Manager

When using the Java Authentication and Authorization Service (JAAS) much of the
configuration depends on which login module is used. However, there are a few
commonalities for every case.  The first place to look is in `bootstrap.xml`.
Here is an example using the `PropertiesLogin` JAAS login module which reads
user, password, and role information from properties files:

```xml
<jaas-security domain="PropertiesLogin"/>
```

No matter what login module you're using, you'll need to specify it here in
`bootstrap.xml`. The `domain` attribute here refers to the relevant login
module entry in `login.config`. For example:

```
PropertiesLogin {
    org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule required
        debug=true
        org.apache.activemq.jaas.properties.user="artemis-users.properties"
        org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};
```

The `login.config` file is a standard JAAS configuration file. You can read
more about this file on [Oracle's
website](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).
In short, the file defines:

- an alias for an entry (e.g. `PropertiesLogin`)

- the implementation class for the login module (e.g.
  `org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule`)

- a flag which indicates whether the success of the login module is `required`,
  `requisite`, `sufficient`, or `optional` (see more details on these flags in
  the
  [JavaDoc](https://docs.oracle.com/javase/8/docs/api/javax/security/auth/login/Configuration.html)

- a list of configuration options specific to the login module implementation

By default, the location and name of `login.config` is specified on the Artemis
command-line which is set by `etc/artemis.profile` on linux and 
`etc\artemis.profile.cmd` on Windows.

#### Dual Authentication

The JAAS Security Manager also supports another configuration parameter -
`certificate-domain`. This is useful when you want to authenticate clients
connecting with SSL connections based on their SSL certificates (e.g. using the
`CertificateLoginModule` discussed below) but you still want to authenticate
clients connecting with non-SSL connections with, e.g., username and password.
Here's an example of what would go in `bootstrap.xml`:

```xml
<jaas-security domain="PropertiesLogin" certificate-domain="CertLogin"/>
```

And here's the corresponding `login.config`:

```
PropertiesLogin {
   org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule required
       debug=false
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};

CertLogin {
   org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule required
       debug=true
       org.apache.activemq.jaas.textfiledn.user="cert-users.properties"
       org.apache.activemq.jaas.textfiledn.role="cert-roles.properties";
};
```

When the broker is configured this way then any client connecting with SSL and
a client certificate will be authenticated using `CertLogin` and any client
connecting without SSL will be authenticated using `PropertiesLogin`.

### JAAS Login Modules

#### GuestLoginModule

Allows users without credentials (and, depending on how it is configured,
possibly also users with invalid credentials) to access the broker. Normally,
the guest login module is chained with another login module, such as a
properties login module. It is implemented by
`org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule`.

- `org.apache.activemq.jaas.guest.user` - the user name to assign; default is "guest"

- `org.apache.activemq.jaas.guest.role` - the role name to assign; default is "guests"

- `credentialsInvalidate` - boolean flag; if `true`, reject login requests that
  include a password (i.e. guest login succeeds only when the user does not
  provide a password); default is `false`

- `debug` - boolean flag; if `true`, enable debugging; this is used only for
  testing or debugging; normally, it should be set to `false`, or omitted;
  default is `false`

There are two basic use cases for the guest login module, as follows:

- Guests with no credentials or invalid credentials.

- Guests with no credentials only.

The following snippet shows how to configure a JAAS login entry for the use
case where users with no credentials or invalid credentials are logged in as
guests. In this example, the guest login module is used in combination with the
properties login module.

```
activemq-domain {
  org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule sufficient
      debug=true
      org.apache.activemq.jaas.properties.user="artemis-users.properties"
      org.apache.activemq.jaas.properties.role="artemis-roles.properties";

  org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule sufficient
      debug=true
      org.apache.activemq.jaas.guest.user="anyone"
      org.apache.activemq.jaas.guest.role="restricted";
};
```

Depending on the user login data, authentication proceeds as follows:

- User logs in with a valid password — the properties login module successfully
  authenticates the user and returns immediately. The guest login module is not
  invoked.

- User logs in with an invalid password — the properties login module fails to
  authenticate the user, and authentication proceeds to the guest login module.
  The guest login module successfully authenticates the user and returns the
  guest principal.

- User logs in with a blank password — the properties login module fails to
  authenticate the user, and authentication proceeds to the guest login module.
  The guest login module successfully authenticates the user and returns the
  guest principal.

The following snipped shows how to configure a JAAS login entry for the use
case where only those users with no credentials are logged in as guests. To
support this use case, you must set the credentialsInvalidate option to true in
the configuration of the guest login module. You should also note that,
compared with the preceding example, the order of the login modules is reversed
and the flag attached to the properties login module is changed to requisite.

```
activemq-guest-when-no-creds-only-domain {
    org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule sufficient
        debug=true
       credentialsInvalidate=true
       org.apache.activemq.jaas.guest.user="guest"
       org.apache.activemq.jaas.guest.role="guests";

    org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule requisite
        debug=true
        org.apache.activemq.jaas.properties.user="artemis-users.properties"
        org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};
```

Depending on the user login data, authentication proceeds as follows:

- User logs in with a valid password — the guest login module fails to
  authenticate the user (because the user has presented a password while the
  credentialsInvalidate option is enabled) and authentication proceeds to the
  properties login module. The properties login module successfully authenticates
  the user and returns.

- User logs in with an invalid password — the guest login module fails to
  authenticate the user and authentication proceeds to the properties login
  module. The properties login module also fails to authenticate the user. The
  net result is authentication failure.

- User logs in with a blank password — the guest login module successfully
  authenticates the user and returns immediately.  The properties login module
  is not invoked.

#### PropertiesLoginModule
The JAAS properties login module provides a simple store of authentication
data, where the relevant user data is stored in a pair of flat files. This is
convenient for demonstrations and testing, but for an enterprise system, the
integration with LDAP is preferable. It is implemented by
`org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule`.

- `org.apache.activemq.jaas.properties.user` - the path to the file which
  contains user and password properties

- `org.apache.activemq.jaas.properties.role` - the path to the file which
  contains user and role properties

- `org.apache.activemq.jaas.properties.password.codec` - the fully qualified
  class name of the password codec to use. See the [password masking](masking-passwords.md)
  documentation for more details on how this works.

- `reload` - boolean flag; whether or not to reload the properties files when a
  modification occurs; default is `false`

- `debug` - boolean flag; if `true`, enable debugging; this is used only for
  testing or debugging; normally, it should be set to `false`, or omitted;
  default is `false`

In the context of the properties login module, the `artemis-users.properties`
file consists of a list of properties of the form, `UserName=Password`. For
example, to define the users `system`, `user`, and `guest`, you could create a
file like the following:

```properties
system=manager
user=password
guest=password
```

Passwords in `artemis-users.properties` can be hashed. Such passwords should
follow the syntax `ENC(<hash>)`. 

Hashed passwords can easily be added to `artemis-users.properties` using the
`user` CLI command from the Artemis *instance*. This command will not work 
from the Artemis home, and it will also not work unless the broker has been
started.

```sh
./artemis user add --user-command-user guest --user-command-password guest --role admin
```

This will use the default codec to perform a "one-way" hash of the password
and alter both the `artemis-users.properties` and `artemis-roles.properties`
files with the specified values.

The `artemis-roles.properties` file consists of a list of properties of the
form, `Role=UserList`, where UserList is a comma-separated list of users. For
example, to define the roles `admins`, `users`, and `guests`, you could create
a file like the following:

```properties
admins=system
users=system,user
guests=guest
```

As mentioned above, the Artemis command-line interface supports a command to
`add` a user. Commands to `list` (one or all) users, `remove` a user, and `reset`
a user's password and/or role(s) are also supported via the command-line
interface as well as the normal management interfaces (e.g. JMX, web console,
etc.).

> **Warning**
>
> Management and CLI operations to manipulate user & role data are only available
> when using the `PropertiesLoginModule`.
>
> In general, using properties files and broker-centric user management for
> anything other than very basic use-cases is not recommended. The broker is
> designed to deal with messages. It's not in the business of managing users,
> although that functionality is provided at a limited level for convenience. LDAP
> is recommended for enterprise level production use-cases.

#### LDAPLoginModule

The LDAP login module enables you to perform authentication and authorization
by checking the incoming credentials against user data stored in a central
X.500 directory server. For systems that already have an X.500 directory server
in place, this means that you can rapidly integrate ActiveMQ Artemis with the
existing security database and user accounts can be managed using the X.500
system. It is implemented by
`org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule`.

- `initialContextFactory` - must always be set to
  `com.sun.jndi.ldap.LdapCtxFactory`

- `connectionURL` - specify the location of the directory server using an ldap
  URL, ldap://Host:Port. You can optionally qualify this URL, by adding a
  forward slash, `/`, followed by the DN of a particular node in the directory
  tree. For example, ldap://ldapserver:10389/ou=system.

- `authentication` - specifies the authentication method used when binding to
  the LDAP server. Can take either of the values, `simple` (username and
  password), `GSSAPI` (Kerberos SASL) or `none` (anonymous).

- `connectionUsername` - the DN of the user that opens the connection to the
  directory server. For example, `uid=admin,ou=system`. Directory servers
  generally require clients to present username/password credentials in order to
  open a connection.

- `connectionPassword` - the password that matches the DN from
  `connectionUsername`. In the directory server, in the DIT, the password is
  normally stored as a `userPassword` attribute in the corresponding directory
  entry.

- `saslLoginConfigScope` - the scope in JAAS configuration (login.config) to
  use to obtain Kerberos initiator credentials when the `authentication` method
  is SASL `GSSAPI`. The default value is `broker-sasl-gssapi`.

- `connectionProtocol` - currently, the only supported value is a blank string.
  In future, this option will allow you to select the Secure Socket Layer (SSL)
  for the connection to the directory server. This option must be set explicitly
  to an empty string, because it has no default value.

- `connectionPool` - boolean, enable the LDAP connection pool property
  'com.sun.jndi.ldap.connect.pool'. Note that the pool is
  [configured at the jvm level with system properties](https://docs.oracle.com/javase/jndi/tutorial/ldap/connect/config.html).

- `connectionTimeout` - specifies the string representation of an integer
  representing the connection timeout in milliseconds. If the LDAP provider
  cannot establish a connection within that period, it aborts the connection
  attempt. The integer should be greater than zero. An integer less than or
  equal to zero means to use the network protocol's (i.e., TCP's) timeout
  value.

  If `connectionTimeout` is not specified, the default is to wait for the
  connection to be established or until the underlying network times out.

  When connection pooling has been requested for a connection, this property
  also determines the maximum wait time for a connection when all connections
  in the pool are in use and the maximum pool size has been reached. If the
  value of this property is less than or equal to zero under such
  circumstances, the provider will wait indefinitely for a connection to
  become available; otherwise, the provider will abort the wait when the
  maximum wait time has been exceeded. See `connectionPool` for more details.

- `readTimeout` - specifies the string representation of an integer representing
  the read timeout in milliseconds for LDAP operations. If the LDAP provider
  cannot get a LDAP response within that period, it aborts the read attempt.
  The integer should be greater than zero. An integer less than or equal to
  zero means no read timeout is specified which is equivalent to waiting for
  the response infinitely until it is received.

  If `readTimeout` is not specified, the default is to wait for the response
  until it is received.

- `userBase` - selects a particular subtree of the DIT to search for user
  entries. The subtree is specified by a DN, which specifes the base node of
  the subtree. For example, by setting this option to
  `ou=User,ou=ActiveMQ,ou=system`, the search for user entries is restricted to
  the subtree beneath the `ou=User,ou=ActiveMQ,ou=system` node.

- `userSearchMatching` - specifies an LDAP search filter, which is applied to
  the subtree selected by `userBase`.  Before passing to the LDAP search
  operation, the string value you provide here is subjected to string
  substitution, as implemented by the `java.text.MessageFormat` class.
  Essentially, this means that the special string, `{0}`, is substituted by the
  username, as extracted from the incoming client credentials.

  After substitution, the string is interpreted as an LDAP search filter,
  where the LDAP search filter syntax is defined by the IETF standard, RFC 2254.
  A short introduction to the search filter syntax is available from Oracle's
  JNDI tutorial, [Search
  Filters](https://docs.oracle.com/javase/jndi/tutorial/basics/directory/filter.html).

  For example, if this option is set to `(uid={0})` and the received username
  is `jdoe`, the search filter becomes `(uid=jdoe)` after string substitution. If
  the resulting search filter is applied to the subtree selected by the user
  base, `ou=User,ou=ActiveMQ,ou=system`, it would match the entry,
  `uid=jdoe,ou=User,ou=ActiveMQ,ou=system` (and possibly more deeply nested
  entries, depending on the specified search depth—see the `userSearchSubtree`
  option).

- `userSearchSubtree` - specify the search depth for user entries, relative to
  the node specified by `userBase`.  This option is a boolean. `false`
  indicates it will try to match one of the child entries of the `userBase` node
  (maps to `javax.naming.directory.SearchControls.ONELEVEL_SCOPE`). `true`
    indicates it will try to match any entry belonging to the subtree of the
  `userBase` node (maps to
  `javax.naming.directory.SearchControls.SUBTREE_SCOPE`).

- `userRoleName` - specifies the name of the multi-valued attribute of the user
  entry that contains a list of role names for the user (where the role names
  are interpreted as group names by the broker's authorization plug-in).  If you
  omit this option, no role names are extracted from the user entry.

- `roleBase` - if you want to store role data directly in the directory server,
  you can use a combination of role options (`roleBase`, `roleSearchMatching`,
  `roleSearchSubtree`, and `roleName`) as an alternative to (or in addition to)
  specifying the `userRoleName` option. This option selects a particular subtree
  of the DIT to search for role/group entries. The subtree is specified by a DN,
  which specifes the base node of the subtree. For example, by setting this
  option to `ou=Group,ou=ActiveMQ,ou=system`, the search for role/group entries
  is restricted to the subtree beneath the `ou=Group,ou=ActiveMQ,ou=system` node.

- `roleName` - specifies the attribute type of the role entry that contains the
  name of the role/group (e.g. C, O, OU, etc.). If you omit this option the
  full DN of the role is used.

- `roleSearchMatching` - specifies an LDAP search filter, which is applied to
  the subtree selected by `roleBase`.  This works in a similar manner to the
  `userSearchMatching` option, except that it supports two substitution strings,
  as follows:

    - `{0}` - substitutes the full DN of the matched user entry (that is, the
      result of the user search). For example, for the user, `jdoe`, the
      substituted string could be `uid=jdoe,ou=User,ou=ActiveMQ,ou=system`.

    - `{1}` - substitutes the received username. For example, `jdoe`.

    For example, if this option is set to `(member=uid={1})` and the received
    username is `jdoe`, the search filter becomes `(member=uid=jdoe)` after string
    substitution (assuming ApacheDS search filter syntax). If the resulting search
    filter is applied to the subtree selected by the role base,
    `ou=Group,ou=ActiveMQ,ou=system`, it matches all role entries that have a
    `member` attribute equal to `uid=jdoe` (the value of a `member` attribute is a
    DN).

    This option must always be set to enable role searching because it has no
    default value. Leaving it unset disables role searching and the role
    information must come from `userRoleName`.

    If you use OpenLDAP, the syntax of the search filter is
    `(member:=uid=jdoe)`.

- `roleSearchSubtree` - specify the search depth for role entries, relative to
  the node specified by `roleBase`.  This option can take boolean values, as
  follows:

    - `false` (default) - try to match one of the child entries of the roleBase
      node (maps to `javax.naming.directory.SearchControls.ONELEVEL_SCOPE`).

    - `true` — try to match any entry belonging to the subtree of the roleBase
      node (maps to `javax.naming.directory.SearchControls.SUBTREE_SCOPE`).

- `authenticateUser` - boolean flag to disable authentication. Useful as an
  optimisation when this module is used just for role mapping of a Subject's
  existing authenticated principals; default is `true`.

- `referral` - specify how to handle referrals; valid values: `ignore`,
  `follow`, `throw`; default is `ignore`.
  
- `ignorePartialResultException` - boolean flag for use when searching Active
  Directory (AD). AD servers don't handle referrals automatically, which causes 
  a `PartialResultException` to be thrown when referrals are encountered by a 
  search, even if `referral` is set to `ignore`. Set to `true` to ignore these 
  exceptions; default is `false`.

- `expandRoles` - boolean indicating whether to enable the role expansion
  functionality or not; default false. If enabled, then roles within roles will
  be found. For example, role `A` is in role `B`. User `X` is in role `A`,
  which means user `X` is in role `B` by virtue of being in role `A`.

- `expandRolesMatching` - specifies an LDAP search filter which is applied to
  the subtree selected by `roleBase`. Before passing to the LDAP search operation,
  the string value you provide here is subjected to string substitution, as
  implemented by the `java.text.MessageFormat` class. Essentially, this means that
  the special string, `{0}`, is substituted by the role name as extracted from the
  previous role search. This option must always be set to enable role expansion
  because it has no default value. Example value: `(member={0})`.

- `debug` - boolean flag; if `true`, enable debugging; this is used only for
  testing or debugging; normally, it should be set to `false`, or omitted;
  default is `false`

Any additional configuration option not recognized by the LDAP login module itself 
is passed as-is to the underlying LDAP connection logic.

Add user entries under the node specified by the `userBase` option. When
creating a new user entry in the directory, choose an object class that
supports the `userPassword` attribute (for example, the `person` or
`inetOrgPerson` object classes are typically suitable). After creating the user
entry, add the `userPassword` attribute, to hold the user's password.

If you want to store role data in dedicated role entries (where each node
represents a particular role), create a role entry as follows. Create a new
child of the `roleBase` node, where the `objectClass` of the child is
`groupOfNames`. Set the `cn` (or whatever attribute type is specified by
`roleName`) of the new child node equal to the name of the role/group. Define a
`member` attribute for each member of the role/group, setting the `member`
value to the DN of the corresponding user (where the DN is specified either
fully, `uid=jdoe,ou=User,ou=ActiveMQ,ou=system`, or partially, `uid=jdoe`).

If you want to add roles to user entries, you would need to customize the
directory schema, by adding a suitable attribute type to the user entry's
object class. The chosen attribute type must be capable of handling multiple
values.

#### CertificateLoginModule

The JAAS certificate authentication login module must be used in combination
with SSL and the clients must be configured with their own certificate. In this
scenario, authentication is actually performed during the SSL/TLS handshake,
not directly by the JAAS certificate authentication plug-in. The role of the
plug-in is as follows:

- To further constrain the set of acceptable users, because only the user DNs
  explicitly listed in the relevant properties file are eligible to be
  authenticated.

- To associate a list of groups with the received user identity, facilitating
  integration with the authorization feature.

- To require the presence of an incoming certificate (by default, the SSL/TLS
  layer is configured to treat the presence of a client certificate as
  optional).

The JAAS certificate login module stores a collection of certificate DNs in a
pair of flat files. The files associate a username and a list of group IDs with
each DN.

The certificate login module is implemented by the following class:

```java
org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule
```

The following `CertLogin` login entry shows how to configure certificate login
module in the login.config file:

```
CertLogin {
    org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule
        debug=true
        org.apache.activemq.jaas.textfiledn.user="users.properties"
        org.apache.activemq.jaas.textfiledn.role="roles.properties";
};
```

In the preceding example, the JAAS realm is configured to use a single
`org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule`
login module. The options supported by this login module are as follows:

- `debug` - boolean flag; if true, enable debugging; this is used only for
  testing or debugging; normally, it should be set to `false`, or omitted;
  default is `false`

- `org.apache.activemq.jaas.textfiledn.user` - specifies the location of the
  user properties file (relative to the directory containing the login
  configuration file).

- `org.apache.activemq.jaas.textfiledn.role` - specifies the location of the
  role properties file (relative to the directory containing the login
  configuration file).

- `reload` - boolean flag; whether or not to reload the properties files when a
  modification occurs; default is `false`

In the context of the certificate login module, the `users.properties` file
consists of a list of properties of the form, `UserName=StringifiedSubjectDN`
or `UserName=/SubjectDNRegExp/`. For example, to define the users, `system`,
`user` and `guest` as well as a `hosts` user matching several DNs, you could
create a file like the following:

```properties
system=CN=system,O=Progress,C=US
user=CN=humble user,O=Progress,C=US
guest=CN=anon,O=Progress,C=DE
hosts=/CN=host\\d+\\.acme\\.com,O=Acme,C=UK/
```

Note that the backslash character has to be escaped because it has a special
treatment in properties files.

Each username is mapped to a subject DN, encoded as a string (where the string
encoding is specified by RFC 2253). For example, the system username is mapped
to the `CN=system,O=Progress,C=US` subject DN. When performing authentication,
the plug-in extracts the subject DN from the received certificate, converts it
to the standard string format, and compares it with the subject DNs in the
`users.properties` file by testing for string equality. Consequently, you must
be careful to ensure that the subject DNs appearing in the `users.properties`
file are an exact match for the subject DNs extracted from the user
certificates.

**Note:** Technically, there is some residual ambiguity in the DN string format.
For example, the `domainComponent` attribute could be represented in a string
either as the string, `DC`, or as the OID, `0.9.2342.19200300.100.1.25`.
Normally, you do not need to worry about this ambiguity. But it could
potentially be a problem, if you changed the underlying implementation of the
Java security layer.

The easiest way to obtain the subject DNs from the user certificates is by
invoking the `keytool` utility to print the certificate contents. To print the
contents of a certificate in a keystore, perform the following steps:

1. Export the certificate from the keystore file into a temporary file. For
   example, to export the certificate with alias `broker-localhost` from the
`broker.ks` keystore file, enter the following command:

   ```sh
   keytool -export -file broker.export -alias broker-localhost -keystore broker.ks -storepass password
   ```

   After running this command, the exported certificate is in the file,
  `broker.export`.

1. Print out the contents of the exported certificate. For example, to print
   out the contents of `broker.export`, enter the following command:

   ```sh
   keytool -printcert -file broker.export
   ```

   Which should produce output similar to that shown here:

   ```
   Owner: CN=localhost, OU=broker, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
   Issuer: CN=localhost, OU=broker, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
   Serial number: 4537c82e
   Valid from: Thu Oct 19 19:47:10 BST 2006 until: Wed Jan 17 18:47:10 GMT 2007
   Certificate fingerprints:
            MD5:  3F:6C:0C:89:A8:80:29:CC:F5:2D:DA:5C:D7:3F:AB:37
            SHA1: F0:79:0D:04:38:5A:46:CE:86:E1:8A:20:1F:7B:AB:3A:46:E4:34:5C
   ```

   The string following `Owner:` gives the subject DN. The format used to enter
  the subject DN depends on your platform. The `Owner:` string above could be
  represented as either `CN=localhost,\ OU=broker,\ O=Unknown,\ L=Unknown,\
  ST=Unknown,\ C=Unknown` or
  `CN=localhost,OU=broker,O=Unknown,L=Unknown,ST=Unknown,C=Unknown`.

The `roles.properties` file consists of a list of properties of the form,
`Role=UserList`, where `UserList` is a comma-separated list of users. For
example, to define the roles `admins`, `users`, and `guests`, you could create
a file like the following:

```properties
admins=system
users=system,user
guests=guest
```


#### SCRAMPropertiesLoginModule
The SCRAM properties login module implements the SASL challenge response for the SCRAM-SHA mechanism.
The data in the properties file reference via `org.apache.activemq.jaas.properties.user` needs to be
generated by the login module it's self, as part of user registration. It contains proof of knowledge
of passwords, rather than passwords themselves. For more usage detail refer to [SCRAM-SHA SASL Mechanism](#SCRAM-SHA-SASL-Mechanism).

```
amqp-sasl-scram {
   org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule required
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};
```

#### SCRAMLoginModule
The SCRAM login module converts a valid SASL SCRAM-SHA Authenticated identity into a JAAS User Principal. This
Principal can then be used for [role mapping](#Role-Mapping).

```
{
   org.apache.activemq.artemis.spi.core.security.jaas.SCRAMLoginModule
};
```

#### ExternalCertificateLoginModule

The external certificate login module is used to propagate a validated TLS client
certificate's subjectDN into a JAAS UserPrincipal. This allows subsequent login modules to
do role mapping for the TLS client certificate.

```
org.apache.activemq.artemis.spi.core.security.jaas.ExternalCertificateLoginModule required
    ;
```    

#### PrincipalConversionLoginModule

The principal conversion login module is used to convert an existing validated Principal 
into a JAAS UserPrincipal. The module is configured with a list of class names used to
match existing Principals. If no UserPrincipal exists, the first matching Principal
will be added as a UserPrincipal of the same Name.

```
org.apache.activemq.artemis.spi.core.security.jaas.PrincipalConversionLoginModule required
     principalClassList=org.apache.x.Principal,org.apache.y.Principal
    ;
```    

#### Krb5LoginModule

The Kerberos login module is used to propagate a validated SASL GSSAPI kerberos token
identity into a validated JAAS UserPrincipal. This allows subsequent login modules to
do role mapping for the kerberos identity.

```
org.apache.activemq.artemis.spi.core.security.jaas.Krb5LoginModule required
    ;
```


The simplest way to make the login configuration available to JAAS is to add
the directory containing the file, `login.config`, to your CLASSPATH.

#### KubernetesLoginModule

The Kubernetes login module enables you to perform authentication and authorization
by validating the `Bearer` token against the Kubernetes API. The authentication is done
by submitting a `TokenReview` request that the Kubernetes cluster validates. The response will
tell whether the user is authenticated and the associated username. It is implemented by `org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModule`.

- `org.apache.activemq.jaas.kubernetes.role` - the path to the file which
  contains user and role mapping

- `reload` - boolean flag; whether or not to reload the properties files when a
  modification occurs; default is `false`

- `debug` - boolean flag; if `true`, enable debugging; this is used only for
  testing or debugging; normally, it should be set to `false`, or omitted;
  default is `false`

The login module must be allowed to query such Rest API. For that, it will use the available
token under `/var/run/secrets/kubernetes.io/serviceaccount/token`. Besides, in order to trust the
connection the client will use the `ca.crt` file existing in the same folder. These two files will
be mounted in the container. The service account running the KubernetesLoginModule must
be allowed to `create::TokenReview`. The `system:auth-delegator` role is typically use for
that purpose.

The `k8s-roles.properties` file consists of a list of properties of the form, `Role=UserList`, where `UserList` is a comma-separated list of users. For example, to define the roles admins, users, and guests, you could create a file like the following:

```properties
admins=system:serviceaccounts:example-ns:admin-sa
users=system:serviceaccounts:other-ns:test-sa
```

### SCRAM-SHA SASL Mechanism

SCRAM (Salted Challenge Response Authentication Mechanism) is an authentication mechanism that can establish mutual
authentication using passwords. Apache ActiveMQ Artemis supports SCRAM-SHA-256 and SCRAM-SHA-512 SASL mechanisms to provide authentication for AMQP connections.

The following properties of SCRAM make it safe to use SCRAM-SHA even on unencrypted connections:

- The passwords are not sent in the clear over the communication channel. The client is challenged to offer proof it knows the password of the authenticating user, and the server is challenged to offer proof it had the password to initialise its authentication store. Only the proof is exchanged.
- The server and client each generate a new challenge for each authentication exchange, making it resilient against replay attacks.


#### Configuring the server to use SCRAM-SHA

The desired SCRAM-SHA mechanisms must be enabled on the AMQP acceptor in
`broker.xml` by adding them to the `saslMechanisms` list url parameter. In this
example, SASL is restricted to only the `SCRAM-SHA-256` mechanism:

````
  <acceptor name="amqp">tcp://localhost:5672?protocols=AMQP;saslMechanisms=SCRAM-SHA-256;saslLoginConfigScope=amqp-sasl-scram
````

Of note is the reference to the sasl login config scope ``saslLoginConfigScope=amqp-sasl-scram`` that holds the relevant SCRAM login module.
The mechanism makes use of JAAS to complete the SASL exchanges.

An example configuration scope for `login.config` that will implement SCRAM-SHA-256 using property files, is as follows:

```
amqp-sasl-scram {
   org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule required
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};
```

#### Configuring a user with SCRAM-SHA data on the server

With SCRAM-SHA, the server's users properties file do not contain any passwords, instead they contain derivative data that
can be used to respond to a challenge.
The secure encoded form of the password must be generated using the main method of
org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule from the artemis-server module and inserting
the resulting lines into your artemis-users.properties file.

````
java -cp "<distro-lib-dir>/*" org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule <username> <password> [<iterations>]
````

An sample of the output can be found in the amqp examples, examples/protocols/amqp/sasl-scram/src/main/resources/activemq/server0/artemis-users.properties

### Kerberos Authentication

You must have the Kerberos infrastructure set up in your deployment environment
before the server can accept Kerberos credentials.  The server can acquire its
Kerberos acceptor credentials by using JAAS and a Kerberos login module. The
JDK provides the
[Krb5LoginModule](https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html)
which executes the necessary Kerberos protocol steps to authenticate and obtain
Kerberos credentials.

#### GSSAPI SASL Mechanism

Using SASL over [AMQP](amqp.md), Kerberos authentication is supported
using the `GSSAPI` SASL mechanism.  With SASL doing Kerberos authentication,
TLS can be used to provide integrity and confidentially to the communications
channel in the normal way.

The `GSSAPI` SASL mechanism must be enabled  on the AMQP acceptor in
`broker.xml` by adding it to the `saslMechanisms` list url parameter:
`saslMechanisms="GSSAPI<,PLAIN, etc>`.

```xml
<acceptor name="amqp">tcp://0.0.0.0:5672?protocols=AMQP;saslMechanisms=GSSAPI</acceptor>
```

The GSSAPI mechanism implementation on the server will use a JAAS configuration
scope named `amqp-sasl-gssapi` to obtain its Kerberos acceptor credentials. An
alternative configuration scope can be specified on the AMQP acceptor using the
url parameter: `saslLoginConfigScope=<some other scope>`.

An example configuration scope for `login.config` that will pick up a Kerberos
keyTab for the Kerberos acceptor Principal `amqp/localhost` is as follows:

```
amqp-sasl-gssapi {
    com.sun.security.auth.module.Krb5LoginModule required
    isInitiator=false
    storeKey=true
    useKeyTab=true
    principal="amqp/localhost"
    debug=true;
};
```

### Role Mapping

On the server, a Kerberos or SCRAM-SHA JAAS authenticated Principal must be added to the
Subject's principal set as an Apache ActiveMQ Artemis UserPrincipal using the
corresponding Apache ActiveMQ Artemis `Krb5LoginModule` or `SCRAMLoginModule` login modules.
They are separate to allow conversion and role mapping to be as restrictive or permissive as desired.

The [PropertiesLoginModule](#propertiesloginmodule) or
[LDAPLoginModule](#ldaploginmodule) can then be used to map the authenticated
 Principal to an Apache ActiveMQ Artemis
[Role](#role-based-security-for-addresses).
Note that in the case of Kerberos, the Peer Principal does not exist as an Apache ActiveMQ Artemis user, only as a role
member.

In the following example, any existing Kerberos authenticated peer will convert to an Apache ActiveMQ Artemis user principal and will
have role mapping applied by the LDAPLoginModule as appropriate.
```
activemq {
  org.apache.activemq.artemis.spi.core.security.jaas.Krb5LoginModule required
    ;
  org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule optional
    initialContextFactory=com.sun.jndi.ldap.LdapCtxFactory
    connectionURL="ldap://localhost:1024"
    authentication=GSSAPI
    saslLoginConfigScope=broker-sasl-gssapi
    connectionProtocol=s
    userBase="ou=users,dc=example,dc=com"
    userSearchMatching="(krb5PrincipalName={0})"
    userSearchSubtree=true
    authenticateUser=false
    roleBase="ou=system"
    roleName=cn
    roleSearchMatching="(member={0})"
    roleSearchSubtree=false
    ;
};
```

### Basic Security Manager

As the name suggests, the `ActiveMQBasicSecurityManager` is _basic_. It is not
pluggable like the JAAS security manager and it _only_ supports authentication
via username and password credentials. Furthermore, the Hawtio-based web
console requires JAAS. Therefore you will *still need* to configure a
`login.config` if you plan on using the web console. However, this security
manager *may* still may have a couple of advantages depending on your use-case.

All user & role data is stored in the bindings journal (or bindings table if
using JDBC). The advantage here is that in a live/backup use-case any user
management performed on the live broker will be reflected on the backup upon
failover.

Typically LDAP would be employed for this kind of use-case, but not everyone
wants or is able to administer an independent LDAP server. One significant
benefit of LDAP is that user data can be shared between multiple live brokers.
However, this is not possible with the `ActiveMQBasicSecurityManager` or, in fact,
any other configuration potentially available out of the box. Nevertheless,
if you just want to share user data between a single live/backup pair then the
basic security manager may be a good fit for you.

User management is provided by the broker's management API. This includes the
ability to add, list, update, and remove users & roles. As with all management
functions, this is available via JMX, management messages, HTTP (via Jolokia),
web console, etc. These functions are also available from the ActiveMQ Artemis
command-line interface. Having the broker store this data directly means that
it must be running in order to manage users. There is no way to modify the
bindings data manually.

To be clear, any management access via HTTP (e.g. web console or Jolokia) will
go through Hawtio JAAS. MBean access via JConsole or other remote JMX tool will
go through the basic security manager. Management messages will also go through
the basic security manager.

#### Configuration

The configuration for the `ActiveMQBasicSecurityManager` happens in
`bootstrap.xml` just like it does for all security manager implementations.
Start by removing `<jaas-security />` section and add `<security-manager />`
configuration as described below.

The `ActiveMQBasicSecurityManager` requires some special configuration for the
following reasons:

 - the bindings data which holds the user & role data cannot be modified
   manually 
 - the broker must be running to manage users
 - the broker often needs to be secured from first boot

If, for example, the broker was configured to use the 
`ActiveMQBasicSecurityManager` and was started from scratch then no clients
would be able to connect because there would be no users & roles configured.
However, in order to configure users & roles one would need to use the 
management API which would require the proper credentials. It's a [catch-22](https://en.wikipedia.org/wiki/Catch-22_(logic))
problem. Therefore, it is essential to configure "bootstrap" credentials that
will be automatically created when the broker starts. There are properties
to define either:

 - a single user whose credentials can then be used to add other users 
 - properties files from which to load users & roles in bulk

Here's an example of the single bootstrap user configuration:

```xml
<broker xmlns="http://activemq.apache.org/schema">

   <security-manager class-name="org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager">
      <property key="bootstrapUser" value="myUser"/>
      <property key="bootstrapPassword" value="myPass"/>
      <property key="bootstrapRole" value="myRole"/>
   </security-manager>
   
   ...
</broker>
```

- `bootstrapUser` - The name of the bootstrap user.
- `bootstrapPassword` - The password for the bootstrap user; supports masking.
- `bootstrapRole` - The role of the bootstrap user.

If your use-case requires *multiple* users to be available when the broker
starts then you can use a configuration like this:

```xml
<broker xmlns="http://activemq.apache.org/schema">

   <security-manager class-name="org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager">
      <property key="bootstrapUserFile" value="artemis-users.properties"/>
      <property key="bootstrapRoleFile" value="artemis-roles.properties"/>
   </security-manager>
   
   ...
</broker>
```

- `bootstrapUserFile` - The name of the file from which to load users. This is
  a *properties* file formatted exactly the same as the user properties file 
  used by the [`PropertiesLoginModule`](#propertiesloginmodule). This file 
  should be on the broker's classpath (e.g. in the `etc` directory).
- `bootstrapRoleFile` - The role of the bootstrap user. This is a *properties*
  file formatted exactly the same as the role properties file used by the
  [`PropertiesLoginModule`](#propertiesloginmodule). This file should be on the
  broker's classpath (e.g. in the `etc` directory).

Regardless of whether you configure a single bootstrap user or load many users
from properties files, any user with which additional users are created should
be in a role with the appropriate permissions on the `activemq.management` 
address. For example if you've specified a `bootstrapUser` then the
`bootstrapRole` will need the following permissions:

- `createNonDurableQueue`
- `createAddress`
- `consume`
- `manage`
- `send`

For example:

```xml
<security-setting match="activemq.management.#">
   <permission type="createNonDurableQueue" roles="myRole"/>
   <permission type="createAddress" roles="myRole"/>
   <permission type="consume" roles="myRole"/>
   <permission type="manage" roles="myRole"/>
   <permission type="send" roles="myRole"/>
</security-setting>
```

> **Note:**
>
> Any `bootstrap` credentials will be reset **whenever** you start the broker
> no matter what changes may have been made to them at runtime previously, so
> depending on your use-case you should decide if you want to leave `bootstrap`
> configuration permanent or if you want to remove it after initial
> configuration.

## Mapping external roles
Roles from external authentication providers (i.e. LDAP) can be mapped to internally used roles. The is done through role-mapping entries in the security-settings block:

```xml
<security-settings>
   [...]
   <role-mapping from="cn=admins,ou=Group,ou=ActiveMQ,ou=system" to="my-admin-role"/>
   <role-mapping from="cn=users,ou=Group,ou=ActiveMQ,ou=system" to="my-user-role"/>
</security-settings>
```

Note: Role mapping is additive. That means the user will keep the original role(s) as well as the newly assigned role(s).

Note: This role mapping only affects the roles which are used to authorize queue access through the configured acceptors. It can not be used to map the role required to access the web console.

## SASL
[AMQP](amqp.md) supports SASL. The following mechanisms are supported:
 PLAIN, EXTERNAL, ANONYMOUS, GSSAPI, SCRAM-SHA-256, SCRAM-SHA-512.
The published list can be constrained via the amqp acceptor `saslMechanisms` property. 
Note: EXTERNAL will only be chosen if a subject is available from the TLS client certificate.

## Changing the username/password for clustering

In order for cluster connections to work correctly, each node in the cluster
must make connections to the other nodes. The username/password they use for
this should always be changed from the installation default to prevent a
security risk.

Please see [Management](management.md) for instructions on how to do this.


## Securing the console

Artemis comes with a web console that allows user to browse Artemis
documentation via an embedded server. By default the web access is plain HTTP.
It is configured in `bootstrap.xml`:

```xml
<web path="web">
    <binding uri="http://localhost:8161">
        <app url="console" war="console.war"/>
    </binding> 
</web>
```

Alternatively you can edit the above configuration to enable secure access
using HTTPS protocol. e.g.:

```xml
<web path="web">
    <binding uri="https://localhost:8443"
             keyStorePath="${artemis.instance}/etc/keystore.jks"
             keyStorePassword="password">
        <app url="jolokia" war="jolokia-war-1.3.5.war"/>
    </binding>
</web>
```

As shown in the example, to enable https the first thing to do is config the
`bind` to be an `https` url. In addition, You will have to configure a few
extra properties described as below.

- `keyStorePath` - The path of the key store file.

- `keyStorePassword` - The key store's password.

- `clientAuth` - The boolean flag indicates whether or not client
  authentication is required. Default is `false`.

- `trustStorePath` - The path of the trust store file. This is needed only if
  `clientAuth` is `true`.

- `trustStorePassword` - The trust store's password.

### Config access using client certificates
The web console supports authentication with client certificates, see the following steps:  

- Add the [certificate login module](#certificateloginmodule) to the `login.config` file, i.e.
```
activemq-cert {
   org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule required
       debug=true
       org.apache.activemq.jaas.textfiledn.user="cert-users.properties"
       org.apache.activemq.jaas.textfiledn.role="cert-roles.properties";
};
```


- Change the hawtio realm to match the realm defined in the `login.config` file
for the [certificate login module](#certificateloginmodule). This is configured in the `artemis.profile` via the system property `-Dhawtio.role=activemq-cert`.


- Create a key pair for the client and import the public key in a truststore file. 
```
keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass securepass -keypass securepass -alias client -genkey -keyalg "RSA" -keysize 2048 -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -ext bc=ca:false -ext eku=cA
keytool -storetype pkcs12 -keystore client-keystore.p12 -storepass securepass -alias client -exportcert -rfc > client.crt
keytool -storetype pkcs12 -keystore client-truststore.p12 -storepass securepass -keypass securepass -importcert -alias client-ca -file client.crt -noprompt
```


- Enable secure access using HTTPS protocol with client authentication,
use the truststore file created in the previous step to set the trustStorePath and trustStorePassword:
```xml
<web path="web">
    <binding uri="https://localhost:8443"
             keyStorePath="${artemis.instance}/etc/server-keystore.p12"
             keyStorePassword="password"
             clientAuth="true"
             trustStorePath="${artemis.instance}/etc/client-truststore.p12"
             trustStorePassword="password">
        <app url="jolokia" war="jolokia-war-1.3.5.war"/>
    </binding>
</web>
```


- Use the private key created in the previous step to set up your client,
i.e. if the client app is a browser install the private key in the browser.

## Controlling JMS ObjectMessage deserialization

Artemis provides a simple class filtering mechanism with which a user can
specify which packages are to be trusted and which are not. Objects whose
classes are from trusted packages can be deserialized without problem, whereas
those from 'not trusted' packages will be denied deserialization.

Artemis keeps a `black list` to keep track of packages that are not trusted and
a `white list` for trusted packages. By default both lists are empty, meaning
any serializable object is allowed to be deserialized. If an object whose class
matches one of the packages in black list, it is not allowed to be
deserialized. If it matches one in the white list the object can be
deserialized. If a package appears in both black list and white list, the one
in black list takes precedence. If a class neither matches with `black list`
nor with the `white list`, the class deserialization will be denied unless the
white list is empty (meaning the user doesn't specify the white list at all).

A class is considered as a 'match' if

- its full name exactly matches one of the entries in the list.
- its package matches one of the entries in the list or is a sub-package of one
  of the entries.

For example, if a class full name is "org.apache.pkg1.Class1", some matching
entries could be:

- `org.apache.pkg1.Class1` - exact match.
- `org.apache.pkg1` - exact package match.
- `org.apache` -- sub package match.

A `*` means 'match-all' in a black or white list.

### Config via Connection Factories

To specify the white and black lists one can use the URL parameters
`deserializationBlackList` and `deserializationWhiteList`. For example, using
JMS:

```java
ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://0?deserializationBlackList=org.apache.pkg1,org.some.pkg2");
```

The above statement creates a factory that has a black list contains two
forbidden packages, "org.apache.pkg1" and "org.some.pkg2", separated by a
comma.

### Config via system properties

There are two system properties available for specifying black list and white
list:

- `org.apache.activemq.artemis.jms.deserialization.whitelist` - comma separated
  list of entries for the white list.
- `org.apache.activemq.artemis.jms.deserialization.blacklist` - comma separated
  list of entries for the black list.

Once defined, all JMS object message deserialization in the VM is subject to
checks against the two lists. However if you create a ConnectionFactory and set
a new set of black/white lists on it, the new values will override the system
properties.

### Config for resource adapters

Message beans using a JMS resource adapter to receive messages can also control
their object deserialization via properly configuring relevant properties for
their resource adapters. There are two properties that you can configure with
connection factories in a resource adapter:

- `deserializationBlackList` - comma separated values for black list
- `deserializationWhiteList` - comma separated values for white list

These properties, once specified, are eventually set on the corresponding
internal factories.

## Masking Passwords

For details about masking passwords in broker.xml please see the [Masking
Passwords](masking-passwords.md) chapter.

## Custom Security Manager

The underpinnings of the broker's security implementation can be changed if so
desired. The broker uses a component called a "security manager" to implement
the actual authentication and authorization checks. By default, the broker uses
`org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager` to
provide JAAS integration, but users can provide their own implementation of
`org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager5` and
configure it in `bootstrap.xml` using the `security-manager` element, e.g.:

```xml
<broker xmlns="http://activemq.apache.org/schema">

   <security-manager class-name="com.foo.MySecurityManager">
      <property key="myKey1" value="myValue1"/>
      <property key="myKey2" value="myValue2"/>
   </security-manager>

   ...
</broker>
```

The `security-manager` example demonstrates how to do this is more detail.

## Per-Acceptor Security Domains

It's possible to override the broker's JAAS security domain by specifying a
security domain on an individual `acceptor`. Simply use the `securityDomain`
parameter and indicate which domain from your `login.config` to use, e.g.:

```xml
<acceptor name="myAcceptor">tcp://127.0.0.1:61616?securityDomain=mySecurityDomain</acceptor>
```

Any client connecting to this acceptor will be have security enforced using
`mySecurityDomain`.
