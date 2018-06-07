# Graceful Server Shutdown

In certain circumstances an administrator might not want to disconnect all
clients immediately when stopping the broker. In this situation the broker can
be configured to shutdown *gracefully* using the `graceful-shutdown-enabled`
boolean configuration parameter.

When the `graceful-shutdown-enabled` configuration parameter is `true` and the
broker is shutdown it will first prevent any additional clients from connecting
and then it will wait for any existing connections to be terminated by the
client before completing the shutdown process. The default value is `false`.

Of course, it's possible a client could keep a connection to the broker
indefinitely effectively preventing the broker from shutting down gracefully.
To deal with this of situation the `graceful-shutdown-timeout` configuration
parameter is available. This tells the broker (in milliseconds) how long to
wait for all clients to disconnect before forcefully disconnecting the clients
and proceeding with the shutdown process. The default value is `-1` which means
the broker will wait indefinitely for clients to disconnect.
