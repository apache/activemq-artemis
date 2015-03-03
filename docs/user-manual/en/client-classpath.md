# The Client Classpath

Apache ActiveMQ requires several jars on the *Client Classpath* depending on
whether the client uses Apache ActiveMQ Core API, JMS, and JNDI.

> **Warning**
>
> All the jars mentioned here can be found in the `lib` directory of the
> Apache ActiveMQ distribution. Be sure you only use the jars from the correct
> version of the release, you *must not* mix and match versions of jars
> from different Apache ActiveMQ versions. Mixing and matching different jar
> versions may cause subtle errors and failures to occur.

## Apache ActiveMQ Core Client

If you are using just a pure Apache ActiveMQ Core client (i.e. no JMS) then you
need `activemq-core-client.jar`, `activemq-commons.jar`, and `netty.jar`
on your client classpath.

## JMS Client

If you are using JMS on the client side, then you will also need to
include `activemq-jms-client.jar` and `geronimo-jms_2.0_spec.jar`.

> **Note**
>
> `geronimo-jms_2.0_spec.jar` just contains Java EE API interface classes needed
> for the `javax.jms.*` classes. If you already have a jar with these
> interface classes on your classpath, you will not need it.
