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
package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.JMSContext;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class JMSSecurityTest extends JMSTestBase {

   @Override
   public boolean useSecurity() {
      return true;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Test
   public void testSecurityOnJMSContext() throws Exception {
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("IDo", "Exist");
      try {
         JMSContext ctx = cf.createContext("Idont", "exist");
         ctx.close();
      } catch (JMSSecurityRuntimeException e) {
         // expected
      }
      JMSContext ctx = cf.createContext("IDo", "Exist");
      ctx.close();
   }

   @Test
   public void testCreateQueueConnection() throws Exception {
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("IDo", "Exist");
      try {
         QueueConnection queueC = ((QueueConnectionFactory) cf).createQueueConnection("IDont", "Exist");
         fail("supposed to throw exception");
         queueC.close();
      } catch (JMSSecurityException e) {
         // expected
      }
      JMSContext ctx = cf.createContext("IDo", "Exist");
      ctx.close();
   }
}
