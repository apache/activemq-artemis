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

package ReplicationTest

import org.apache.activemq.artemis.api.core.QueueConfiguration
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl
import org.apache.activemq.artemis.core.server.ActiveMQServer

import java.nio.charset.Charset

evaluate(new File('ReplicationTest/testConfiguration.groovy'))
server = server as ActiveMQServer

waitForCondition("Waiting up to 10 seconds for \"${server.configuration.name}\" to become active ...",
      "Server \"${server.configuration.name}\" failed to activate on time.", 10, server.&isActive)

ServerLocatorImpl.newLocator("tcp://${masterBindAddress}:${masterBindPort}").withCloseable { locator ->
   locator.blockOnDurableSend = true
   locator.createSessionFactory().withCloseable { sf ->
      sf.createSession(true, true).withCloseable { session ->
         session.start()
         session.createQueue(new QueueConfiguration(replicationTestQueueName as String))
         session.createProducer(replicationTestQueueName as String).withCloseable { producer ->
            ClientMessage message = session.createMessage(true)
            message.writeBodyBufferBytes(replicationTestString.bytes)
            println "Sending message \"${replicationTestString}\" to the master server ..."
            producer.send(message)
         }
      }
   }
}

waitForCondition("Waiting up to 10 seconds for \"${server.configuration.name}\" to synchronize ...",
      "Server \"${server.configuration.name}\" failed to synchronize on time.", 10, server.&isReplicaSync)

queue = server.locateQueue(replicationTestQueueName)
assertEquals("Test queue on \"${server.configuration.name}\" is expected to contain one message.", 1L, queue.durableMessageCount)
browser = queue.browserIterator()
assertTrue("Test queue on \"${server.configuration.name}\" is expected to contain a message.", browser.hasNext())
message = browser.next()
assertEquals("The message with ID ${message.messageID} received by \"${server.configuration.name}\" doesn't match.", replicationTestString, message.message.toCore().readOnlyBodyBuffer.byteBuf().toString(Charset.defaultCharset()))

println "Testing message \"$replicationTestString\" with ID ${message.messageID} has been successfully delivered to \"${server.configuration.name}\"."