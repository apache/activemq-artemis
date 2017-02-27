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
package org.apache.activemq.artemis.tests.integration.openwire;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.HashSet;
import java.util.Set;

public class SecurityOpenWireTest extends BasicOpenWireTest {

   @Override
   @Before
   public void setUp() throws Exception {
      //this system property is used to construct the executor in
      //org.apache.activemq.transport.AbstractInactivityMonitor.createExecutor()
      //and affects the pool's shutdown time. (default is 30 sec)
      //set it to 2 to make tests shutdown quicker.
      System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.keepAliveTime", "2");
      this.realStore = true;
      enableSecurity = true;
      super.setUp();
   }

   @Override
   protected void extraServerConfig(Configuration serverConfig) {

      super.extraServerConfig(serverConfig);
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("denyQ", "denyQ");
      securityManager.getConfiguration().addRole("denyQ", "denyQ");
   }

   @Test
   public void testSendNoAuth() throws Exception {
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", false, false, false, false, false, false, false, false, false, false));

      server.getSecurityRepository().addMatch("denyQ", roles);
      SimpleString denyQ = new SimpleString("denyQ");
      server.createQueue(denyQ, RoutingType.ANYCAST, denyQ, null, true, false);
      try (Connection connection = factory.createConnection("denyQ", "denyQ")) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("denyQ");
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         try {
            producer.send(session.createTextMessage());
            fail();
         } catch (JMSException e) {
            //pass
         }
      }
   }

}
