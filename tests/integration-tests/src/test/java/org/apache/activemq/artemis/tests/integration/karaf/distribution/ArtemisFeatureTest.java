/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.karaf.distribution;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import java.util.concurrent.Callable;

import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.tests.integration.karaf.KarafBaseTest;
import org.apache.log4j.Logger;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.ProbeBuilder;
import org.ops4j.pax.exam.TestProbeBuilder;
import org.ops4j.pax.exam.junit.PaxExam;
import org.osgi.framework.Constants;

/**
 * Useful docs about this test: https://ops4j1.jira.com/wiki/display/paxexam/FAQ
 */
@RunWith(PaxExam.class)
public class ArtemisFeatureTest extends KarafBaseTest {

   private static Logger LOG = Logger.getLogger(ArtemisFeatureTest.class.getName());

   @ProbeBuilder
   public TestProbeBuilder probeConfiguration(TestProbeBuilder probe) {
      probe.setHeader(Constants.DYNAMICIMPORT_PACKAGE, "*,org.ops4j.pax.exam.options.*,org.apache.felix.service.*;status=provisional");
      return probe;
   }

   @Configuration
   public Option[] configure() throws Exception {
      return configureArtemisFeatures(false, null, "artemis");
   }
   @Test
   public void testSample() throws Throwable {
      System.out.println("Hello!!!");
      ClientMessageImpl message = new ClientMessageImpl();
   }

   @Test(timeout = 5 * 60 * 1000)
   public void test() throws Throwable {
      executeCommand("bundle:list");

      withinReason(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            assertTrue("artemis bundle installed", verifyBundleInstalled("artemis-server-osgi"));
            return true;
         }
      });

      Object service = waitForService("(objectClass=org.apache.activemq.artemis.core.server.ActiveMQServer)", 30000);
      assertNotNull(service);
      LOG.info("have service " + service);

      executeCommand("service:list -n");

      Connection connection = null;
      try {
         JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
         connection = factory.createConnection(USER, PASSWORD);
         connection.start();

         javax.jms.Session sess = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
         Queue queue = sess.createQueue("exampleQueue");
         MessageProducer producer = sess.createProducer(queue);
         producer.send(sess.createTextMessage("TEST"));

         MessageConsumer consumer = sess.createConsumer(queue);
         Message msg = consumer.receive(5000);
         assertNotNull(msg);
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}
