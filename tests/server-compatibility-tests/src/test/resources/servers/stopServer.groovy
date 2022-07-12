package servers

import org.apache.activemq.artemis.core.server.ActiveMQServer
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ

if (!binding.hasVariable('server')) {
    println 'No server to stop.'
    return
}

if (!server.metaClass.getMetaMethod('stop')) {
    println 'The "server" property doesn\'t contain a "stop()" method.'
    return server
}

server.stop()

if (server instanceof EmbeddedActiveMQ) {
    server = server.activeMQServer
}

if (server !instanceof ActiveMQServer) {
    println "The \"server\" property is not a supported server. Not waiting for it to stop."
    return server
}

server = server as ActiveMQServer
if (server.state != ActiveMQServer.SERVER_STATE.STOPPED) {
    println "Waiting up to 10 seconds for the server \"${server.configuration.name}\" to stop ..."
    10.times {
        directive = server.state == ActiveMQServer.SERVER_STATE.STOPPED ? 1 : 0
        Thread.sleep(1000)
    }
    assertEquals(ActiveMQServer.SERVER_STATE.STOPPED, server.state)
    println "Server \"${server.configuration.name}\" stopped."
}