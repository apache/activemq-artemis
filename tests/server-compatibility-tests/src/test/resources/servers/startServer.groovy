package servers

import org.apache.activemq.artemis.core.server.ActiveMQServer
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ

if (!binding.hasVariable('server')) {
    println 'No server to start.'
    return
}

if (!server.metaClass.getMetaMethod('start')) {
    println 'The "server" property doesn\'t contain a "start()" method.'
    return server
}

server.start()

if (server instanceof EmbeddedActiveMQ) {
    server = server.activeMQServer
}

if (server !instanceof ActiveMQServer) {
    println "The \"server\" property is not a supported server. Not waiting for it to start."
    return server
}

server = server as ActiveMQServer

println "Waiting up to 10 seconds for the server \"${server.configuration.name}\" to start ..."

10.times {
    directive = server.state == ActiveMQServer.SERVER_STATE.STARTED ? 1 : 0
    Thread.sleep(1000)
}

assertEquals(ActiveMQServer.SERVER_STATE.STARTED, server.state)

println "Server \"${server.configuration.name}\" started."