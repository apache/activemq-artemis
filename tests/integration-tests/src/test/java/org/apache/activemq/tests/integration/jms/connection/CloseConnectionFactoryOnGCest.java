/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.jms.connection;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.tests.util.JMSTestBase;

/**
 *
 * A CloseConnectionOnGCTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CloseConnectionFactoryOnGCest extends JMSTestBase
{

   public void testCloseCFOnGC() throws Exception
   {

      final AtomicInteger valueGC = new AtomicInteger(0);

      ServerLocatorImpl.finalizeCallback = new Runnable()
      {
         public void run()
         {
            valueGC.incrementAndGet();
         }
      };

      try
      {
         // System.setOut(out);
         for (int i = 0; i < 100; i++)
         {
            HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                            new TransportConfiguration("org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory"));
            Connection conn = cf.createConnection();
            cf = null;
            conn.close();
            conn = null;
         }
         forceGC();
      }
      finally
      {
         ServerLocatorImpl.finalizeCallback = null;
      }

      assertEquals("The code is throwing exceptions", 0, valueGC.get());

   }
}
