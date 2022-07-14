/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

waitForCondition("Waiting up to 10 seconds for the server \"${server.configuration.name}\" to stop ...",
      "Server \"${server.configuration.name}\" failed to stop on time.", 10) {
   server.state == ActiveMQServer.SERVER_STATE.STOPPED
}

println "Server \"${server.configuration.name}\" stopped."
