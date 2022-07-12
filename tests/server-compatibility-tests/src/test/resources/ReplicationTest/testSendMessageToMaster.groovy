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
import org.apache.tools.ant.filters.StringInputStream

import java.nio.charset.Charset

evaluate(new File('ReplicationTest/testConfiguration.groovy'))
server = server as ActiveMQServer

waitForCondition(10, server.&isActive)

ServerLocatorImpl.newLocator("tcp://${masterBindAddress}:${masterBindPort}").withCloseable { locator ->
    locator.blockOnDurableSend = true
    locator.createSessionFactory().withCloseable { sf ->
        sf.createSession(true, true).withCloseable { session ->
            session.start()
            session.createQueue(new QueueConfiguration(replicationTestQueueName as String))
            session.createProducer(replicationTestQueueName as String).withCloseable { producer ->
                new StringInputStream(replicationTestString).withCloseable { body ->
                    ClientMessage message = session.createMessage(true)
                    message.bodyInputStream = body
                    producer.send(message)
                }
            }
        }
    }
}

waitForCondition(10, server.&isReplicaSync)

queue = server.locateQueue(replicationTestQueueName)
assertEquals(1L, queue.durableMessageCount)
browser = queue.browserIterator()
assertTrue(browser.hasNext())
message = browser.next()
assertEquals(replicationTestString, message.message.toCore().readOnlyBodyBuffer.byteBuf().toString(Charset.defaultCharset()))
