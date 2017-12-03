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
package org.apache.activemq.artemis.tests.integration.jms.connection;

import javax.jms.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Test;

/**
 * A CloseConnectionOnGCTest
 */
public class CloseConnectionFactoryOnGCTest extends JMSTestBase {

   @Test(timeout = 60000)
   public void testCloseCFOnGC() throws Exception {

      final AtomicInteger valueGC = new AtomicInteger(0);

      ServerLocatorImpl.finalizeCallback = new Runnable() {
         @Override
         public void run() {
            valueGC.incrementAndGet();
         }
      };

      try {
         // System.setOut(out);
         for (int i = 0; i < 100; i++) {
            ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
            Connection conn = cf.createConnection();
            cf = null;
            conn.close();
            conn = null;
         }
         forceGC();
      } finally {
         ServerLocatorImpl.finalizeCallback = null;
      }

      assertEquals("The code is throwing exceptions", 0, valueGC.get());

   }
}
