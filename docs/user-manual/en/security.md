# Security

This chapter describes how security works with Apache ActiveMQ Artemis and how
you can configure it. To disable security completely simply set the
`security-enabled` property to false in the `broker.xml` file.

For performance reasons security is cached and invalidated every so long. To
change this period set the property `security-invalidation-interval`, which is
in milliseconds. The default is `10000` ms.

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

Eight different permissions can be given to the set of queues which match the
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
  `deleteDurableQueue`, `createNonDurableQueue`, `deleteNonDurableQueue`

As mentioned, there are a few places where a translation was performed to
achieve some equivalence.:

- This mapping doesn't include the Artemis `manage` permission type since there
  is no type analogous for that in ActiveMQ 5.x.

- The `admin` permission in ActiveMQ 5.x relates to whether or not the broker
  will auto-create a destination if it doesn't exist and the user sends a
  message to it. Artemis automatically allows the automatic creation of a
  destination if the user has permission to send message to it. Therefore, the
  plugin will map the `admin` permission to the 4 aforementioned permissions in
  Artemis.

## Secure Sockets Layer (SSL) Transport

When messaging clients are connected to servers, or servers are connected to
other servers (e.g. via bridges) over an untrusted network then Apache ActiveMQ
Artemis allows that traffic to be encrypted using the Secure Sockets Layer
(SSL) transport.

For more information on configuring the SSL transport, please see [Configuring
the Transport](configuring-transports.md).

## User credentials

Apache ActiveMQ Artemis ships with two security manager implementations:

- The legacy, deprecated `ActiveMQSecurityManager` that reads user credentials,
  i.e. user names, passwords and role information from properties files on the
  classpath called `artemis-users.properties` and `artemis-roles.properties`.

- The flexible, pluggable `ActiveMQJAASSecurityManager` which supports any
  standard JAAS login module. Artemis ships with several login modules which
  will be discussed further down. This is the default security manager.

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
follow the syntax `ENC(<hash>)`. Hashed passwords can easily be added to
`artemis-users.properties` using the `user` CLI command, e.g.:

```sh
./artemis user add --username guest --password guest --role admin
```

This will use the default
`org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec` to perform a
"one-way" hash of the password and alter both the `artemis-users.properties`
and `artemis-roles.properties` files with the specified values. 

The `artemis-roles.properties` file consists of a list of properties of the
form, `Role=UserList`, where UserList is a comma-separated list of users. For
example, to define the roles `admins`, `users`, and `guests`, you could create
a file like the following:

```properties
admins=system
users=system,user
guests=guest
```

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

- `connectionPool`. boolean, enable the ldap connection pool property
 'com.sun.jndi.ldap.connect.pool'. Note that the pool is [configured at the jvm level with system properties](https://docs.oracle.com/javase/jndi/tutorial/ldap/connect/config.html).


- `connectionTimeout`. String milliseconds, that can time limit a ldap connection
 attempt. The default is infinite.

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
  existing authenticated principals; default is `false`.

- `referral` - specify how to handle referrals; valid values: `ignore`,
  `follow`, `throw`; default is `ignore`.

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

-   `debug` - boolean flag; if true, enable debugging; this is used only for testing or debugging; normally,
    it should be set to `false`, or omitted; default is `false`

-   `org.apache.activemq.jaas.textfiledn.user` - specifies the location of the user properties file (relative to the
     directory containing the login configuration file).

-   `org.apache.activemq.jaas.textfiledn.role` - specifies the location of the role properties file (relative to the
    directory containing the login configuration file).

-   `reload` - boolean flag; whether or not to reload the properties files when a modification occurs; default is `false`

In the context of the certificate login module, the `users.properties` file consists of a list of properties of the form,
`UserName=StringifiedSubjectDN` or `UserName=/SubjectDNRegExp/`. For example, to define the users, `system`, `user` and
`guest` as well as a `hosts` user matching several DNs, you could create a file like the following:

    system=CN=system,O=Progress,C=US
    user=CN=humble user,O=Progress,C=US
    guest=CN=anon,O=Progress,C=DE
    hosts=/CN=host\\d+\\.acme\\.com,O=Acme,C=UK/

Note that the backslash character has to be escaped because it has a special treatment in properties files.

Each username is mapped to a subject DN, encoded as a string (where the string encoding is specified by RFC 2253). For
example, the system username is mapped to the `CN=system,O=Progress,C=US` subject DN. When performing authentication,
the plug-in extracts the subject DN from the received certificate, converts it to the standard string format, and
compares it with the subject DNs in the `users.properties` file by testing for string equality. Consequently, you must
be careful to ensure that the subject DNs appearing in the `users.properties` file are an exact match for the subject
DNs extracted from the user certificates.

- `org.apache.activemq.jaas.textfiledn.user` - specifies the location of the
  user properties file (relative to the directory containing the login
  configuration file).

- `org.apache.activemq.jaas.textfiledn.role` - specifies the location of the
  role properties file (relative to the directory containing the login
  configuration file).

- `reload` - boolean flag; whether or not to reload the properties files when a
  modification occurs; default is `false`

In the context of the certificate login module, the `users.properties` file
consists of a list of properties of the form, `UserName=StringifiedSubjectDN`.
For example, to define the users, system, user, and guest, you could create a
file like the following:

```properties
system=CN=system,O=Progress,C=US
user=CN=humble user,O=Progress,C=US
guest=CN=anon,O=Progress,C=DE
```

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

The simplest way to make the login configuration available to JAAS is to add
the directory containing the file, `login.config`, to your CLASSPATH.

### Kerberos Authentication

You must have the Kerberos infrastructure set up in your deployment environment
before the server can accept Kerberos credentials.  The server can acquire its
Kerberos acceptor credentials by using JAAS and a Kerberos login module. The
JDK provides the
[Krb5LoginModule](https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html)
which executes the necessary Kerberos protocol steps to authenticate and obtain
Kerberos credentials.

#### GSSAPI SASL Mechanism

Using SASL over [AMQP](using-AMQP.md), Kerberos authentication is supported
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
scope named `amqp-sasl-gssapi` to obtain it's Kerberos acceptor credentials. An
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

#### Role Mapping

On the server, the Kerberos authenticated Peer Principal can be added to the
Subject's principal set as an Apache ActiveMQ Artemis UserPrincipal using the
Apache ActiveMQ Artemis `Krb5LoginModule` login module. The
[PropertiesLoginModule](#propertiesloginmodule) or
[LDAPLoginModule](#ldaploginmodule) can then be used to map the authenticated
Kerberos Peer Principal to an Apache ActiveMQ Artemis
[Role](#role-based-security-for-addresses). Note that the Kerberos Peer
Principal does not exist as an Apache ActiveMQ Artemis user, only as a role
member.

```
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
```

#### TLS Kerberos Cipher Suites

The legacy [rfc2712](https://www.ietf.org/rfc/rfc2712.txt) defines TLS Kerberos
cipher suites that can be used by TLS to negotiate Kerberos authentication. The
cypher suites offered by rfc2712 are dated and insecure and rfc2712 has been
superseded by SASL GSSAPI. However, for clients that don't support SASL (core
client), using TLS can provide Kerberos authentication over an *unsecure*
channel.


## SASL
[AMQP](using-AMQP.md) supports SASL. The following mechanisms are supported; PLAIN, EXTERNAL, ANONYMOUS, GSSAPI.
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
<web bind="http://localhost:8161" path="web">
    <app url="console" war="console.war"/>
</web>
```

Alternatively you can edit the above configuration to enable secure access
using HTTPS protocol. e.g.:

```xml
<web bind="https://localhost:8443"
    path="web"
    keyStorePath="${artemis.instance}/etc/keystore.jks"
    keyStorePassword="password">
    <app url="jolokia" war="jolokia-war-1.3.5.war"/>
</web>
```

As shown in the example, to enable https the first thing to do is config the
`bind` to be an `https` url. In addition, You will have to configure a few
extra properties desribed as below.

- `keyStorePath` - The path of the key store file.

- `keyStorePassword` - The key store's password.

- `clientAuth` - The boolean flag indicates whether or not client
  authentication is required. Default is `false`.

- `trustStorePath` - The path of the trust store file. This is needed only if
  `clientAuth` is `true`.

- `trustStorePassword` - The trust store's password.

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

### Config for REST interface

Apache Artemis REST interface ([Rest](rest.md)) allows interactions between jms
client and rest clients.  It uses JMS ObjectMessage to wrap the actual user
data between the 2 types of clients and deserialization is needed during this
process. If you want to control the deserialization for REST, you need to set
the black/white lists for it separately as Apache Artemis REST Interface is
deployed as a web application.  You need to put the black/white lists in its
web.xml, as context parameters, as follows

```xml
<web-app>
    <context-param>
        <param-name>org.apache.activemq.artemis.jms.deserialization.whitelist</param-name>
        <param-value>some.allowed.class</param-value>
    </context-param>
    <context-param>
        <param-name>org.apache.activemq.artemis.jms.deserialization.blacklist</param-name>
        <param-value>some.forbidden.class</param-value>
    </context-param>
...
</web-app>
```

The param-value for each list is a comma separated string value representing the list.

## Masking Passwords

For details about masking passwords in broker.xml please see the [Masking
Passwords](masking-passwords.md) chapter.
