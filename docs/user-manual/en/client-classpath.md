# The Client Classpath

Apache ActiveMQ Artemis requires just a single jar on the *client classpath*.

> **Warning**
>
> The client jar mentioned here can be found in the `lib/client` directory of the
> Apache ActiveMQ Artemis distribution. Be sure you only use the jar from the correct
> version of the release, you *must not* mix and match versions of jars
> from different Apache ActiveMQ Artemis versions. Mixing and matching different jar
> versions may cause subtle errors and failures to occur.

Whether you are using JMS or just the Core API simply add the `artemis-jms-client-all.jar`
from the `lib/client` directory to your client classpath. This is a "shaded" jar that
contains all the Artemis code plus dependencies (e.g. JMS spec, Netty, etc.).