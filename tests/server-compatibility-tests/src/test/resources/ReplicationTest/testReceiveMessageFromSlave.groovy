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

import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl
import org.apache.activemq.artemis.core.server.ActiveMQServer

evaluate(new File('ReplicationTest/testConfiguration.groovy'))
server = server as ActiveMQServer

waitForCondition("Waiting up to 10 seconds for \"${server.configuration.name}\" to become active ...", 10, server.&isActive)
waitForCondition("Waiting up to 10 seconds for \"${server.configuration.name}\" to synchronize ...", 10, server.&isReplicaSync)

ServerLocatorImpl.newLocator("tcp://${slaveBindAddress}:${slaveBindPort}").withCloseable { locator ->
   locator.createSessionFactory().withCloseable { sf ->
      sf.createSession(true, false).withCloseable { session ->
         session.start()
         session.createConsumer(replicationTestQueueName as String).withCloseable { consumer ->
            ClientMessage message = consumer.receive(5000)
            assertNotNull(message)
            final os = new ByteArrayOutputStream()
            os.withCloseable {
               message.saveToOutputStream(it)
            }
            assertEquals(replicationTestString, os.toString())
         }
         session.commit()
      }
   }
}
