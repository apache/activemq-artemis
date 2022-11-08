/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.upgradeTest;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** This test is making sure the upgrade command would be able to upgrade a test I created with artemis 2.25.0 */
public class UpgradeTest extends SmokeTestBase {

   File upgradedServer;

   Process processServer;

   @Before
   public void beforeTest() throws Exception {
      upgradedServer = new File(basedir + "/target/classes/servers/linuxUpgrade");
      deleteDirectory(new File(upgradedServer, "data"));
      deleteDirectory(new File(upgradedServer, "log"));

      processServer = ServerUtil.startServer(upgradedServer.getAbsolutePath(), "upgradedServer", 0, 5000);
      addProcess(processServer);
   }

   @Test
   public void testSimpleSendReceive() throws Throwable {

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");

      try (Connection connection = factory.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(session.createQueue(getName()));
         MessageProducer producer = session.createProducer(session.createQueue(getName()));
         String randomString = "Hello " + RandomUtil.randomString();
         producer.send(session.createTextMessage(randomString));
         TextMessage message = (TextMessage)consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(randomString, message.getText());
      }

   }

}
