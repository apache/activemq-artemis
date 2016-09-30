/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Test;

public class RedeployTest extends ActiveMQTestBase {

   @Test
   public void testRedeploy() throws Exception {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL url1 = RedeployTest.class.getClassLoader().getResource("reload-test-jms.xml");
      URL url2 = RedeployTest.class.getClassLoader().getResource("reload-test-updated-jms.xml");
      Files.copy(url1.openStream(), brokerXML);

      EmbeddedJMS embeddedJMS = new EmbeddedJMS();
      embeddedJMS.setConfigResourcePath(brokerXML.toUri().toString());
      embeddedJMS.start();

      final ReusableLatch latch = new ReusableLatch(1);

      Runnable tick = new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      };

      embeddedJMS.getActiveMQServer().getReloadManager().setTick(tick);

      try {
         latch.await(10, TimeUnit.SECONDS);
         Assert.assertEquals("jms.queue.DLQ", embeddedJMS.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getDeadLetterAddress().toString());
         Assert.assertEquals("jms.queue.ExpiryQueue", embeddedJMS.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getExpiryAddress().toString());
         Assert.assertFalse(tryConsume());
         Files.copy(url2.openStream(), brokerXML, StandardCopyOption.REPLACE_EXISTING);
         brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);
         latch.setCount(1);
         embeddedJMS.getActiveMQServer().getReloadManager().setTick(tick);
         latch.await(10, TimeUnit.SECONDS);

         Assert.assertTrue(tryConsume());

         Assert.assertEquals("jms.queue.NewQueue", embeddedJMS.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getDeadLetterAddress().toString());
         Assert.assertEquals("jms.queue.NewQueue", embeddedJMS.getActiveMQServer().getAddressSettingsRepository().getMatch("jms").getExpiryAddress().toString());

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Queue queue = session.createQueue("DivertQueue");
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("text"));
            connection.start();
            MessageConsumer consumer = session.createConsumer(session.createQueue("NewQueue"));
            Assert.assertNotNull("Divert wasn't redeployed accordingly", consumer.receive(5000));
         }

      } finally {
         embeddedJMS.stop();
      }
   }

   private boolean tryConsume() throws JMSException {
      try (ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
           Connection connection = factory.createConnection();
           Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE)) {
         Queue queue = session.createQueue("NewQueue");
         MessageConsumer consumer = session.createConsumer(queue);
         return true;
      } catch (JMSException e) {
         return false;
      }

   }
}
