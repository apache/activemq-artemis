# Security

This chapter describes how security works with Apache ActiveMQ Artemis and how you can
configure it. To disable security completely simply set the
`security-enabled` property to false in the `broker.xml`
file.

For performance reasons security is cached and invalidated every so
long. To change this period set the property
`security-invalidation-interval`, which is in milliseconds. The default
is `10000` ms.

## Role based security for addresses

Apache ActiveMQ Artemis contains a flexible role-based security model for applying
security to queues, based on their addresses.

As explained in [Using Core](using-core.md), Apache ActiveMQ Artemis core consists mainly of sets of queues bound
to addresses. A message is sent to an address and the server looks up
the set of queues that are bound to that address, the server then routes
the message to those set of queues.

Apache ActiveMQ Artemis allows sets of permissions to be defined against the queues
based on their address. An exact match on the address can be used or a
wildcard match can be used using the wildcard characters '`#`' and
'`*`'.

Seven different permissions can be given to the set of queues which
match the address. Those permissions are:

-   `createDurableQueue`. This permission allows the user to create a
    durable queue under matching addresses.

-   `deleteDurableQueue`. This permission allows the user to delete a
    durable queue under matching addresses.

-   `createNonDurableQueue`. This permission allows the user to create a
    non-durable queue under matching addresses.

-   `deleteNonDurableQueue`. This permission allows the user to delete a
    non-durable queue under matching addresses.

-   `send`. This permission allows the user to send a message to
    matching addresses.

-   `consume`. This permission allows the user to consume a message from
    a queue bound to matching addresses.

-   `manage`. This permission allows the user to invoke management
    operations by sending management messages to the management address.

For each permission, a list of roles who are granted that permission is
specified. If the user has any of those roles, he/she will be granted
that permission for that set of addresses.

Let's take a simple example, here's a security block from
`broker.xml` file:

    <security-setting match="globalqueues.europe.#">
       <permission type="createDurableQueue" roles="admin"/>
       <permission type="deleteDurableQueue" roles="admin"/>
       <permission type="createNonDurableQueue" roles="admin, guest, europe-users"/>
       <permission type="deleteNonDurableQueue" roles="admin, guest, europe-users"/>
       <permission type="send" roles="admin, europe-users"/>
       <permission type="consume" roles="admin, europe-users"/>
    </security-setting>

The '`#`' character signifies "any sequence of words". Words are
delimited by the '`.`' character. For a full description of the wildcard
syntax please see [Understanding the Wildcard Syntax](wildcard-syntax.md).
The above security block applies to any address
that starts with the string "globalqueues.europe.":

Only users who have the `admin` role can create or delete durable queues
bound to an address that starts with the string "globalqueues.europe."

Any users with the roles `admin`, `guest`, or `europe-users` can create
or delete temporary queues bound to an address that starts with the
string "globalqueues.europe."

Any users with the roles `admin` or `europe-users` can send messages to
these addresses or consume messages from queues bound to an address that
starts with the string "globalqueues.europe."

The mapping between a user and what roles they have is handled by the
security manager. Apache ActiveMQ Artemis ships with a user manager that reads user
credentials from a file on disk, and can also plug into JAAS or JBoss
Application Server security.

For more information on configuring the security manager, please see 'Changing the Security Manager'.

There can be zero or more `security-setting` elements in each xml file.
Where more than one match applies to a set of addresses the *more
specific* match takes precedence.

Let's look at an example of that, here's another `security-setting`
block:

    <security-setting match="globalqueues.europe.orders.#">
       <permission type="send" roles="europe-users"/>
       <permission type="consume" roles="europe-users"/>
    </security-setting>

In this `security-setting` block the match
'globalqueues.europe.orders.\#' is more specific than the previous match
'globalqueues.europe.\#'. So any addresses which match
'globalqueues.europe.orders.\#' will take their security settings *only*
from the latter security-setting block.

Note that settings are not inherited from the former block. All the
settings will be taken from the more specific matching block, so for the
address 'globalqueues.europe.orders.plastics' the only permissions that
exist are `send` and `consume` for the role europe-users. The
permissions `createDurableQueue`, `deleteDurableQueue`,
`createNonDurableQueue`, `deleteNonDurableQueue` are not inherited from
the other security-setting block.

By not inheriting permissions, it allows you to effectively deny
permissions in more specific security-setting blocks by simply not
specifying them. Otherwise it would not be possible to deny permissions
in sub-groups of addresses.

## Security Setting Plugin

Aside from configuring sets of permissions via XML these permissions can also be
configured via plugins which implement `org.apache.activemq.artemis.core.server.SecuritySettingPlugin`.
One or more plugins can be defined and configured alongside the normal XML, e.g.:

    <security-settings>
       ...
       <security-setting-plugin class-name="org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin">
          <setting name="initialContextFactory" value="com.sun.jndi.ldap.LdapCtxFactory"/>
          <setting name="connectionURL" value="ldap://localhost:1024"/>
          <setting name="connectionUsername" value="uid=admin,ou=system"/>
          <setting name="connectionPassword" value="secret"/>
          <setting name="connectionProtocol" value="s"/>
          <setting name="authentication" value="simple"/>
       </security-setting-plugin>
    </security-settings>

Most of this configuration is specific to the plugin implementation. However, there are two configuration details that
will be specified for every implementation:

-   `class-name`. This attribute of `security-setting-plugin` indicates the name of the class which implements
    `org.apache.activemq.artemis.core.server.SecuritySettingPlugin`.

-   `setting`. Each of these elements represents a name/value pair that will be passed to the implementation for configuration
    purposes.

See the JavaDoc on `org.apache.activemq.artemis.core.server.SecuritySettingPlugin` for further details about the interface
and what each method is expected to do.

### Available plugins

#### LegacyLDAPSecuritySettingPlugin

This plugin will read the security information that was previously handled by [`LDAPAuthorizationMap`](http://activemq.apache.org/security.html)
and the [`cachedLDAPAuthorizationMap`](http://activemq.apache.org/cached-ldap-authorization-module.html) in Apache ActiveMQ 5.x
and turn it into Artemis security settings where possible. The security implementations of ActiveMQ 5.x and Artemis don't
match perfectly so some translation must occur to achieve near equivalent functionality.

Here is an example of the plugin's configuration:

    <security-setting-plugin class-name="org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin">
       <setting name="initialContextFactory" value="com.sun.jndi.ldap.LdapCtxFactory"/>
       <setting name="connectionURL" value="ldap://localhost:1024"/>
       <setting name="connectionUsername" value="uid=admin,ou=system"/>
       <setting name="connectionPassword" value="secret"/>
       <setting name="connectionProtocol" value="s"/>
       <setting name="authentication" value="simple"/>
    </security-setting-plugin>

-   `class-name`. The implementation is `org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin`.

-   `initialContextFactory`. The initial context factory used to connect to LDAP. It must always be set to
    `com.sun.jndi.ldap.LdapCtxFactory` (i.e. the default value).

-   `connectionURL`. Specifies the location of the directory server using an ldap URL, `ldap://Host:Port`. You can
    optionally qualify this URL, by adding a forward slash, `/`, followed by the DN of a particular node in the directory
    tree. For example, `ldap://ldapserver:10389/ou=system`. The default is `ldap://localhost:1024`.

-   `connectionUsername`. The DN of the user that opens the connection to the directory server. For example, `uid=admin,ou=system`.
    Directory servers generally require clients to present username/password credentials in order to open a connection.

-   `connectionPassword`. The password that matches the DN from `connectionUsername`. In the directory server, in the
    DIT, the password is normally stored as a `userPassword` attribute in the corresponding directory entry.

-   `connectionProtocol`. Currently the only supported value is a blank string. In future, this option will allow you to
    select the Secure Socket Layer (SSL) for the connection to the directory server. Note: this option must be set
    explicitly to an empty string, because it has no default value.

-   `authentication`. Specifies the authentication method used when binding to the LDAP server. Can take either of the
     values, `simple` (username and password, the default value) or `none` (anonymous). Note: Simple Authentication and
     Security Layer (SASL) authentication is currently not supported.

-   `destinationBase`. Specifies the DN of the node whose children provide the permissions for all destinations. In this
    case the DN is a literal value (that is, no string substitution is performed on the property value).  For example, a
    typical value of this property is `ou=destinations,o=ActiveMQ,ou=system` (i.e. the default value).

-   `filter`. Specifies an LDAP search filter, which is used when looking up the permissions for any kind of destination.
    The search filter attempts to match one of the children or descendants of the queue or topic node. The default value
    is `(cn=*)`.

-   `roleAttribute`. Specifies an attribute of the node matched by `filter`, whose value is the DN of a role. Default
    value is `uniqueMember`.

-   `adminPermissionValue`. Specifies a value that matches the `admin` permission. The default value is `admin`.

-   `readPermissionValue`. Specifies a value that matches the `read` permission. The default value is `read`.

-   `writePermissionValue`. Specifies a value that matches the `write` permission. The default value is `write`.

The name of the queue or topic defined in LDAP will serve as the "match" for the security-setting, the permission value
will be mapped from the ActiveMQ 5.x type to the Artemis type, and the role will be mapped as-is. It's worth noting that
since the name of queue or topic coming from LDAP will server as the "match" for the security-setting the security-setting
may not be applied as expected to JMS destinations since Artemis always prefixes JMS destinations with "jms.queue." or
"jms.topic." as necessary.

ActiveMQ 5.x only has 3 permission types - `read`, `write`, and `admin`. These permission types are described on their
[website](http://activemq.apache.org/security.html). However, as described previously, ActiveMQ Artemis has 6 permission
types - `createDurableQueue`, `deleteDurableQueue`, `createNonDurableQueue`, `deleteNonDurableQueue`, `send`, `consume`,
and `manage`. Here's how the old types are mapped to the new types:

-   `read` - `consume`
-   `write` - `send`
-   `admin` - `createDurableQueue`, `deleteDurableQueue`, `createNonDurableQueue`, `deleteNonDurableQueue`

As mentioned, there are a few places where a translation was performed to achieve some equivalence.:

-   This mapping doesn't include the Artemis `manage` permission type since there is no type analogous for that in ActiveMQ
    5.x.

-   The `admin` permission in ActiveMQ 5.x relates to whether or not the broker will auto-create a destination if
    it doesn't exist and the user sends a message to it. Artemis automatically allows the automatic creation of a
    destination if the user has permission to send message to it. Therefore, the plugin will map the `admin` permission
    to the 4 aforementioned permissions in Artemis.

## Secure Sockets Layer (SSL) Transport

When messaging clients are connected to servers, or servers are
connected to other servers (e.g. via bridges) over an untrusted network
then Apache ActiveMQ Artemis allows that traffic to be encrypted using the Secure
Sockets Layer (SSL) transport.

For more information on configuring the SSL transport, please see [Configuring the Transport](configuring-transports.md).

## User credentials

Apache ActiveMQ Artemis ships with two security manager implementations:
 
-   The legacy, deprecated `ActiveMQSecurityManager` that reads user credentials, i.e. user names, passwords and role 
information from properties files on the classpath called `artemis-users.properties` and `artemis-roles.properties`. 
This is the default security manager.

-   The flexible, pluggable `ActiveMQJAASSecurityManager` which supports any standard JAAS login module. Artemis ships 
with several login modules which will be discussed further down. 

### Non-JAAS Security Manager

If you wish to use the legacy, deprecated `ActiveMQSecurityManager`, then it needs to be added to the `bootstrap.xml` 
configuration. Lets take a look at what this might look like:

    <basic-security>
      <users>file:${activemq.home}/config/non-clustered/artemis-users.properties</users>
      <roles>file:${activemq.home}/config/non-clustered/artemis-roles.properties</roles>
      <default-user>guest</default-user>
    </basic-security>

The first 2 elements `users` and `roles` define what properties files should be used to load in the users and passwords.

The next thing to note is the element `defaultuser`. This defines what user will be assumed when the client does not 
specify a username/password when creating a session. In this case they will be the user `guest`. Multiple roles can be 
specified for a default user in the `artemis-roles.properties`.

Lets now take a look at the `artemis-users.properties` file, this is basically just a set of key value pairs that define
the users and their password, like so:

    bill=activemq
    andrew=activemq1
    frank=activemq2
    sam=activemq3

The `artemis-roles.properties` defines what groups these users belong too where the key is the user and the value is a 
comma separated list of the groups the user belongs to, like so:

    bill=user
    andrew=europe-user,user
    frank=us-user,news-user,user
    sam=news-user,user
    
### JAAS Security Manager

When using JAAS much of the configuration depends on which login module is used. However, there are a few commonalities
for every case. Just like in the non-JAAS use-case, the first place to look is in `bootstrap.xml`. Here is an example
using the `PropertiesLogin` JAAS login module which reads user, password, and role information from properties files
much like the non-JAAS security manager implementation:

    <jaas-security login-module="PropertiesLogin"/>
    
No matter what login module you're using, you'll need to specify it here in `bootstrap.xml`. The `login-module` attribute
here refers to the relevant login module entry in `login.config`. For example:

    PropertiesLogin {
        org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule required
            debug=true
            org.apache.activemq.jaas.properties.user="artemis-users.properties"
            org.apache.activemq.jaas.properties.role="artemis-roles.properties";
    };

The `login.config` file is a standard JAAS configuration file. You can read more about this file on 
[Oracle's website](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).
In short, the file defines:

-   an alias for a configuration (e.g. `PropertiesLogin`)

-   the implementation class (e.g. `org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule`)

-   a flag which indicates whether the success of the LoginModule is `required`, `requisite`, `sufficient`, or `optional`

-   a list of configuration options specific to the login module implementation

By default, the location and name of `login.config` is specified on the Artemis command-line which is set by 
`etc/artemis.profile` on linux and `etc\artemis.profile.cmd` on Windows.

### JAAS Login Modules

#### GuestLoginModule
Allows users without credentials (and, depending on how it is configured, possibly also users with invalid credentials) 
to access the broker. Normally, the guest login module is chained with another login module, such as a properties login 
module. It is implemented by `org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule`.

-   `org.apache.activemq.jaas.guest.user` - the user name to assign; default is "guest"

-   `org.apache.activemq.jaas.guest.role` - the role name to assign; default is "guests"

-   `credentialsInvalidate` - boolean flag; if `true`, reject login requests that include a password (i.e. guest login
succeeds only when the user does not provide a password); default is `false`

-   `debug` - boolean flag; if `true`, enable debugging; this is used only for testing or debugging; normally, it 
should be set to `false`, or omitted; default is `false`

There are two basic use cases for the guest login module, as follows:

-   Guests with no credentials or invalid credentials.

-   Guests with no credentials only.

The following snippet shows how to configure a JAAS login entry for the use case where users with no credentials or 
invalid credentials are logged in as guests. In this example, the guest login module is used in combination with the 
properties login module.

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

Depending on the user login data, authentication proceeds as follows:

-   User logs in with a valid password — the properties login module successfully authenticates the user and returns 
    immediately. The guest login module is not invoked.

-   User logs in with an invalid password — the properties login module fails to authenticate the user, and authentication 
    proceeds to the guest login module. The guest login module successfully authenticates the user and returns the guest principal.

-   User logs in with a blank password — the properties login module fails to authenticate the user, and authentication 
    proceeds to the guest login module. The guest login module successfully authenticates the user and returns the guest principal.

The following snipped shows how to configure a JAAS login entry for the use case where only those users with no 
credentials are logged in as guests. To support this use case, you must set the credentialsInvalidate option to true in 
the configuration of the guest login module. You should also note that, compared with the preceding example, the order 
of the login modules is reversed and the flag attached to the properties login module is changed to requisite.

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

Depending on the user login data, authentication proceeds as follows:

-   User logs in with a valid password — the guest login module fails to authenticate the user (because the user has 
    presented a password while the credentialsInvalidate option is enabled) and authentication proceeds to the properties 
    login module. The properties login module successfully authenticates the user and returns.

-   User logs in with an invalid password — the guest login module fails to authenticate the user and authentication proceeds
    to the properties login module. The properties login module also fails to authenticate the user. The nett result is 
    authentication failure.
 
-   User logs in with a blank password — the guest login module successfully authenticates the user and returns immediately.
    The properties login module is not invoked.
    
#### PropertiesLoginModule
The JAAS properties login module provides a simple store of authentication data, where the relevant user data is stored 
in a pair of flat files. This is convenient for demonstrations and testing, but for an enterprise system, the integration 
with LDAP is preferable. It is implemented by `org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule`.

-   `org.apache.activemq.jaas.properties.user` - the path to the file which contains user and password properties

-   `org.apache.activemq.jaas.properties.role` - the path to the file which contains user and role properties

-   `debug` - boolean flag; if `true`, enable debugging; this is used only for testing or debugging; normally, it 
should be set to `false`, or omitted; default is `false`

In the context of the properties login module, the `artemis-users.properties` file consists of a list of properties of the 
form, `UserName=Password`. For example, to define the users `system`, `user`, and `guest`, you could create a file like 
the following:

    system=manager
    user=password
    guest=password

The `artemis-roles.properties` file consists of a list of properties of the form, `Role=UserList`, where UserList is a 
comma-separated list of users. For example, to define the roles `admins`, `users`, and `guests`, you could create a file 
like the following:

    admins=system
    users=system,user
    guests=guest
    
#### LDAPLoginModule
The LDAP login module enables you to perform authentication and authorization by checking the incoming credentials against 
user data stored in a central X.500 directory server. For systems that already have an X.500 directory server in place, 
this means that you can rapidly integrate ActiveMQ Artemis with the existing security database and user accounts can be 
managed using the X.500 system. It is implemented by `org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule`.

-   `initialContextFactory` - must always be set to `com.sun.jndi.ldap.LdapCtxFactory`    

-   `connectionURL` - specify the location of the directory server using an ldap URL, ldap://Host:Port. You can 
    optionally qualify this URL, by adding a forward slash, `/`, followed by the DN of a particular node in the directory
    tree. For example, ldap://ldapserver:10389/ou=system.    
        
-   `authentication` - specifies the authentication method used when binding to the LDAP server. Can take either of 
    the values, `simple` (username and password) or `none` (anonymous). 
            
-   `connectionUsername` - the DN of the user that opens the connection to the directory server. For example, 
    `uid=admin,ou=system`. Directory servers generally require clients to present username/password credentials in order
    to open a connection.      
      
-   `connectionPassword` - the password that matches the DN from `connectionUsername`. In the directory server, 
    in the DIT, the password is normally stored as a `userPassword` attribute in the corresponding directory entry.    
         
-   `connectionProtocol` - currently, the only supported value is a blank string. In future, this option will allow 
    you to select the Secure Socket Layer (SSL) for the connection to the directory server. This option must be set 
    explicitly to an empty string, because it has no default value.        
    
-   `userBase` - selects a particular subtree of the DIT to search for user entries. The subtree is specified by a 
    DN, which specifes the base node of the subtree. For example, by setting this option to `ou=User,ou=ActiveMQ,ou=system`,
    the search for user entries is restricted to the subtree beneath the `ou=User,ou=ActiveMQ,ou=system` node.   
         
-   `userSearchMatching` - specifies an LDAP search filter, which is applied to the subtree selected by `userBase`. 
    Before passing to the LDAP search operation, the string value you provide here is subjected to string substitution, 
    as implemented by the `java.text.MessageFormat` class. Essentially, this means that the special string, `{0}`, is 
    substituted by the username, as extracted from the incoming client credentials.  
      
    After substitution, the string is interpreted as an LDAP search filter, where the LDAP search filter syntax is 
    defined by the IETF standard, RFC 2254. A short introduction to the search filter syntax is available from Oracle's 
    JNDI tutorial, [Search Filters](http://download.oracle.com/javase/jndi/tutorial/basics/directory/filter.html). 
       
    For example, if this option is set to `(uid={0})` and the received username is `jdoe`, the search filter becomes
    `(uid=jdoe)` after string substitution. If the resulting search filter is applied to the subtree selected by the 
    user base, `ou=User,ou=ActiveMQ,ou=system`, it would match the entry, `uid=jdoe,ou=User,ou=ActiveMQ,ou=system`
    (and possibly more deeply nested entries, depending on the specified search depth—see the `userSearchSubtree` option). 
           
-   `userSearchSubtree` - specify the search depth for user entries, relative to the node specified by `userBase`. 
    This option is a boolean. `false` indicates it will try to match one of the child entries of the `userBase` node 
    (maps to `javax.naming.directory.SearchControls.ONELEVEL_SCOPE`). `true` indicates it will try to match any entry 
    belonging to the subtree of the `userBase` node (maps to `javax.naming.directory.SearchControls.SUBTREE_SCOPE`). 
           
-   `userRoleName` - specifies the name of the multi-valued attribute of the user entry that contains a list of 
    role names for the user (where the role names are interpreted as group names by the broker's authorization plug-in).
    If you omit this option, no role names are extracted from the user entry.         
    
-   `roleBase` - if you want to store role data directly in the directory server, you can use a combination of role 
    options (`roleBase`, `roleSearchMatching`, `roleSearchSubtree`, and `roleName`) as an alternative to (or in addition 
    to) specifying the `userRoleName` option. This option selects a particular subtree of the DIT to search for role/group 
    entries. The subtree is specified by a DN, which specifes the base node of the subtree. For example, by setting this 
    option to `ou=Group,ou=ActiveMQ,ou=system`, the search for role/group entries is restricted to the subtree beneath 
    the `ou=Group,ou=ActiveMQ,ou=system` node.        
    
-   `roleName` - specifies the attribute type of the role entry that contains the name of the role/group (e.g. C, O,
    OU, etc.). If you omit this option, the role search feature is effectively disabled.        
    
-   `roleSearchMatching` - specifies an LDAP search filter, which is applied to the subtree selected by `roleBase`. 
    This works in a similar manner to the `userSearchMatching` option, except that it supports two substitution strings,
    as follows:        
    
    -   `{0}` - substitutes the full DN of the matched user entry (that is, the result of the user search). For 
        example, for the user, `jdoe`, the substituted string could be `uid=jdoe,ou=User,ou=ActiveMQ,ou=system`.
        
    -   `{1}` - substitutes the received username. For example, `jdoe`.    
    
    For example, if this option is set to `(member=uid={1})` and the received username is `jdoe`, the search filter 
    becomes `(member=uid=jdoe)` after string substitution (assuming ApacheDS search filter syntax). If the resulting 
    search filter is applied to the subtree selected by the role base, `ou=Group,ou=ActiveMQ,ou=system`, it matches all 
    role entries that have a `member` attribute equal to `uid=jdoe` (the value of a `member` attribute is a DN).  
      
    This option must always be set, even if role searching is disabled, because it has no default value. 
       
    If you use OpenLDAP, the syntax of the search filter is `(member:=uid=jdoe)`.
    
-   `roleSearchSubtree` - specify the search depth for role entries, relative to the node specified by `roleBase`. 
    This option can take boolean values, as follows:
    
    -   `false` (default) - try to match one of the child entries of the roleBase node (maps to 
        `javax.naming.directory.SearchControls.ONELEVEL_SCOPE`).    
                                   
    -   `true` — try to match any entry belonging to the subtree of the roleBase node (maps to 
        `javax.naming.directory.SearchControls.SUBTREE_SCOPE`).

-   `debug` - boolean flag; if `true`, enable debugging; this is used only for testing or debugging; normally, it 
should be set to `false`, or omitted; default is `false`

Add user entries under the node specified by the `userBase` option. When creating a new user entry in the directory, 
choose an object class that supports the `userPassword` attribute (for example, the `person` or `inetOrgPerson` object 
classes are typically suitable). After creating the user entry, add the `userPassword` attribute, to hold the user's 
password.

If you want to store role data in dedicated role entries (where each node represents a particular role), create a role 
entry as follows. Create a new child of the `roleBase` node, where the `objectClass` of the child is `groupOfNames`. Set 
the `cn` (or whatever attribute type is specified by `roleName`) of the new child node equal to the name of the 
role/group. Define a `member` attribute for each member of the role/group, setting the `member` value to the DN of the 
corresponding user (where the DN is specified either fully, `uid=jdoe,ou=User,ou=ActiveMQ,ou=system`, or partially, 
`uid=jdoe`).

If you want to add roles to user entries, you would need to customize the directory schema, by adding a suitable 
attribute type to the user entry's object class. The chosen attribute type must be capable of handling multiple values.

## Changing the username/password for clustering

In order for cluster connections to work correctly, each node in the
cluster must make connections to the other nodes. The username/password
they use for this should always be changed from the installation default
to prevent a security risk.

Please see [Management](management.md) for instructions on how to do this.
