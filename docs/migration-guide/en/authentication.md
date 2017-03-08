Authentication
=====================================

Now that we have our acceptors and addresses ready, it's time to deal with broker security. Artemis inherited most of the security concepts from ActiveMQ. One of the most notable differences is that ActiveMQ *groups* are now called *roles* in Artemis. Besides that things should be pretty familiar to existing ActiveMQ users. Let's start by looking into the authentication mechanisms and defining users and roles (groups).
 
 Both ActiveMQ and Artemis use JAAS to define authentication credentials. In ActiveMQ, that's configured through the appropriate broker plugin in `conf/activemq.xml`

```xml
<plugins>
  <jaasAuthenticationPlugin configuration="activemq" />
</plugins>
```
    
The name of the JAAS domain is specified as a configuration parameter.    
    
In Artemis, the same thing is achieved by defining `<jaas-security>` configuration in `etc/bootstrap.xml`

```xml
<jaas-security domain="activemq"/>
```
    
From this point on, you can go and define your users and their roles in appropriate files, like `conf/users.properties` and `conf/groups.properties` in ActiveMQ. Similarly, `etc/artemis-users.properties` and `etc/artemis-roles.properties` files are used in Artemis. These files are interchangeable, so you should be able to just copy your existing configuration over to the new broker. 

If your deployment is more complicated that this and requires some advanced JAAS configuration, you'll need go and change the `etc/login.config` file. It's important to say that all custom JAAS modules and configuration you were using in ActiveMQ should be compatible with Artemis.

Finally, in case you're still using ActiveMQ's *Simple Authentication Plugin*, which defines users and groups directly in the broker's xml configuration file, you'll need to migrate to JAAS as Artemis doesn't support the similar concept.
