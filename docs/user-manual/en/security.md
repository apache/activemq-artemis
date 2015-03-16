# Security

This chapter describes how security works with Apache ActiveMQ and how you can
configure it. To disable security completely simply set the
`security-enabled` property to false in the `activemq-configuration.xml`
file.

For performance reasons security is cached and invalidated every so
long. To change this period set the property
`security-invalidation-interval`, which is in milliseconds. The default
is `10000` ms.

## Role based security for addresses

Apache ActiveMQ contains a flexible role-based security model for applying
security to queues, based on their addresses.

As explained in [Using Core](using-core.md), Apache ActiveMQ core consists mainly of sets of queues bound
to addresses. A message is sent to an address and the server looks up
the set of queues that are bound to that address, the server then routes
the message to those set of queues.

Apache ActiveMQ allows sets of permissions to be defined against the queues
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
`activemq-configuration.xml` file:

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
security manager. Apache ActiveMQ ships with a user manager that reads user
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

## Secure Sockets Layer (SSL) Transport

When messaging clients are connected to servers, or servers are
connected to other servers (e.g. via bridges) over an untrusted network
then Apache ActiveMQ allows that traffic to be encrypted using the Secure
Sockets Layer (SSL) transport.

For more information on configuring the SSL transport, please see [Configuring the Transport](configuring-transports.md).

## Basic user credentials

Apache ActiveMQ ships with a security manager implementation that reads user
credentials, i.e. user names, passwords and role information from properties
files on the classpath called `activemq-users.properties` and `activemq-roles.properties`. This is the default security manager.

If you wish to use this security manager, then users, passwords and
roles can easily be added into these files.

To configure this manager then it needs to be added to the `bootstrap.xml` configuration.
Lets take a look at what this might look like:

    <basic-security>
      <users>file:${activemq.home}/config/non-clustered/activemq-users.properties</users>
      <roles>file:${activemq.home}/config/non-clustered/activemq-roles.properties</roles>
      <default-user>guest</default-user>
    </basic-security>

The first 2 elements `users` and `roles` define what properties files should be used to load in the users and passwords.

The next thing to note is the element `defaultuser`. This defines what
user will be assumed when the client does not specify a
username/password when creating a session. In this case they will be the
user `guest`. Multiple roles can be specified for a default user in the
`activemq-roles.properties`.

Lets now take alook at the `activemq-users.properties` file, this is basically
just a set of key value pairs that define the users and their password, like so:

    bill=activemq
    andrew=activemq1
    frank=activemq2
    sam=activemq3

The `activemq-roles.properties` defines what groups these users belong too
where the key is the user and the value is a comma seperated list of the groups
the user belongs to, like so:

    bill=user
    andrew=europe-user,user
    frank=us-user,news-user,user
    sam=news-user,user

## Changing the username/password for clustering

In order for cluster connections to work correctly, each node in the
cluster must make connections to the other nodes. The username/password
they use for this should always be changed from the installation default
to prevent a security risk.

Please see [Management](management.md) for instructions on how to do this.
