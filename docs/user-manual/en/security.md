Security
========

This chapter describes how security works with ActiveMQ and how you can
configure it. To disable security completely simply set the
`security-enabled` property to false in the `activemq-configuration.xml`
file.

For performance reasons security is cached and invalidated every so
long. To change this period set the property
`security-invalidation-interval`, which is in milliseconds. The default
is `10000` ms.

Role based security for addresses
=================================

ActiveMQ contains a flexible role-based security model for applying
security to queues, based on their addresses.

As explained in ?, ActiveMQ core consists mainly of sets of queues bound
to addresses. A message is sent to an address and the server looks up
the set of queues that are bound to that address, the server then routes
the message to those set of queues.

ActiveMQ allows sets of permissions to be defined against the queues
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
`activemq-configuration.xml` or `activemq-queues.xml` file:

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
syntax please see ?. The above security block applies to any address
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
security manager. ActiveMQ ships with a user manager that reads user
credentials from a file on disk, and can also plug into JAAS or JBoss
Application Server security.

For more information on configuring the security manager, please see ?.

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

Secure Sockets Layer (SSL) Transport
====================================

When messaging clients are connected to servers, or servers are
connected to other servers (e.g. via bridges) over an untrusted network
then ActiveMQ allows that traffic to be encrypted using the Secure
Sockets Layer (SSL) transport.

For more information on configuring the SSL transport, please see ?.

Basic user credentials
======================

ActiveMQ ships with a security manager implementation that reads user
credentials, i.e. user names, passwords and role information from an xml
file on the classpath called `activemq-users.xml`. This is the default
security manager.

If you wish to use this security manager, then users, passwords and
roles can easily be added into this file.

Let's take a look at an example file:

    <configuration xmlns="urn:activemq"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="urn:activemq ../schemas/activemq-users.xsd ">

       <defaultuser name="guest" password="guest">
          <role name="guest"/>
       </defaultuser>

       <user name="tim" password="marmite">
          <role name="admin"/>
       </user>

       <user name="andy" password="doner_kebab">
          <role name="admin"/>
          <role name="guest"/>
       </user>

       <user name="jeff" password="camembert">
          <role name="europe-users"/>
          <role name="guest"/>
       </user>

    </configuration>

The first thing to note is the element `defaultuser`. This defines what
user will be assumed when the client does not specify a
username/password when creating a session. In this case they will be the
user `guest` and have the role also called `guest`. Multiple roles can
be specified for a default user.

We then have three more users, the user `tim` has the role `admin`. The
user `andy` has the roles `admin` and `guest`, and the user `jeff` has
the roles `europe-users` and `guest`.

Changing the security manager
=============================

If you do not want to use the default security manager then you can
specify a different one by editing the file `activemq-beans.xml` (or
`activemq-jboss-beans.xml` if you're running JBoss Application Server)
and changing the class for the `ActiveMQSecurityManager` bean.

Let's take a look at a snippet from the default beans file:

               
    <bean name="ActiveMQSecurityManager" class="org.apache.activemq.spi.core.security.ActiveMQSecurityManagerImpl">
       <start ignored="true"/>
       <stop ignored="true"/>
    </bean>

The class
`org.apache.activemq.spi.core.security.ActiveMQSecurityManagerImpl` is
the default security manager that is used by the standalone server.

ActiveMQ ships with two other security manager implementations you can
use off-the-shelf; one a JAAS security manager and another for
integrating with JBoss Application Sever security, alternatively you
could write your own implementation by implementing the
`org.apache.activemq.spi.core.security.ActiveMQSecurityManager`
interface, and specifying the classname of your implementation in the
file `activemq-beans.xml` (or `activemq-jboss-beans.xml` if you're
running JBoss Application Server).

These two implementations are discussed in the next two sections.

JAAS Security Manager
=====================

JAAS stands for 'Java Authentication and Authorization Service' and is a
standard part of the Java platform. It provides a common API for
security authentication and authorization, allowing you to plugin your
pre-built implementations.

To configure the JAAS security manager to work with your pre-built JAAS
infrastructure you need to specify the security manager as a
`JAASSecurityManager` in the beans file. Here's an example:

    <bean name="ActiveMQSecurityManager" class="org.apache.activemq.integration.jboss.security.JAASSecurityManager">
       <start ignored="true"/>
       <stop ignored="true"/>

       <property name="ConfigurationName">org.apache.activemq.jms.example.ExampleLoginModule</property>
       <property name="Configuration">
          <inject bean="ExampleConfiguration"/>
       </property>
       <property name="CallbackHandler">
          <inject bean="ExampleCallbackHandler"/>
       </property>
    </bean>

Note that you need to feed the JAAS security manager with three
properties:

-   ConfigurationName: the name of the `LoginModule` implementation that
    JAAS must use

-   Configuration: the `Configuration` implementation used by JAAS

-   CallbackHandler: the `CallbackHandler` implementation to use if user
    interaction are required

Example
-------

See ? for an example which shows how ActiveMQ can be configured to use
JAAS.

JBoss AS Security Manager
=========================

The JBoss AS security manager is used when running ActiveMQ inside the
JBoss Application server. This allows tight integration with the JBoss
Application Server's security model.

The class name of this security manager is
`org.apache.activemq.integration.jboss.security.JBossASSecurityManager`

Take a look at one of the default `activemq-jboss-beans.xml` files for
JBoss Application Server that are bundled in the distribution for an
example of how this is configured.

Configuring Client Login
------------------------

JBoss can be configured to allow client login, basically this is when a
JEE component such as a Servlet or EJB sets security credentials on the
current security context and these are used throughout the call. If you
would like these credentials to be used by ActiveMQ when sending or
consuming messages then set `allowClientLogin` to true. This will bypass
ActiveMQ authentication and propagate the provided Security Context. If
you would like ActiveMQ to authenticate using the propagated security
then set the `authoriseOnClientLogin` to true also.

There is more info on using the JBoss client login module
[here](http://community.jboss.org/wiki/ClientLoginModule)

> **Note**
>
> If messages are sent non blocking then there is a chance that these
> could arrive on the server after the calling thread has completed
> meaning that the security context has been cleared. If this is the
> case then messages will need to be sent blocking

Changing the Security Domain
----------------------------

The name of the security domain used by the JBoss AS security manager
defaults to `java:/jaas/activemq
          `. This can be changed by specifying `securityDomainName`
(e.g. java:/jaas/myDomain).

Changing the username/password for clustering
=============================================

In order for cluster connections to work correctly, each node in the
cluster must make connections to the other nodes. The username/password
they use for this should always be changed from the installation default
to prevent a security risk.

Please see ? for instructions on how to do this.
