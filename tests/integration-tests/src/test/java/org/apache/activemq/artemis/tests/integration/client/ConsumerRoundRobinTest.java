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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerRoundRobinTest extends ActiveMQTestBase {

   private static final Logger log = Logger.getLogger(ConsumerRoundRobinTest.class);

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   @Test
   public void testConsumersRoundRobinCorrectly() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setDurable(false));

      ClientConsumer[] consumers = new ClientConsumer[5];
      // start the session before we create the consumers, this is because start is non blocking and we have to
      // guarantee
      // all consumers have been started before sending messages
      session.start();
      consumers[0] = session.createConsumer(queueA);
      consumers[1] = session.createConsumer(queueA);
      consumers[2] = session.createConsumer(queueA);
      consumers[3] = session.createConsumer(queueA);
      consumers[4] = session.createConsumer(queueA);

      ClientProducer cp = session.createProducer(addressA);
      int numMessage = 10;
      for (int i = 0; i < numMessage; i++) {
         ClientMessage cm = session.createMessage(false);
         cm.getBodyBuffer().writeInt(i);
         cp.send(cm);
      }
      int currMessage = 0;
      for (int i = 0; i < numMessage / 5; i++) {
         log.debug("i is " + i);
         for (int j = 0; j < 5; j++) {
            log.debug("j is " + j);
            ClientMessage cm = consumers[j].receive(5000);
            Assert.assertNotNull(cm);
            Assert.assertEquals(currMessage++, cm.getBodyBuffer().readInt());
            cm.acknowledge();
         }
      }
   }

}
