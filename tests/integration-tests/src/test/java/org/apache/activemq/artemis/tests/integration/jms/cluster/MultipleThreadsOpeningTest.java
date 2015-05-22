/**
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
package org.apache.activemq.artemis.tests.integration.jms.cluster;

import javax.jms.Connection;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.tests.util.JMSClusteredTestBase;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.junit.Test;

public class MultipleThreadsOpeningTest extends JMSClusteredTestBase
{

   @Test
   public void testMultipleOpen() throws Exception
   {
      cf1 = ActiveMQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                          generateInVMParams(1)));

      final int numberOfOpens = 2000;
      int numberOfThreads = 20;
      // I want all the threads aligned, just ready to start creating connections like in a car race
      final CountDownLatch flagAlignSemaphore = new CountDownLatch(numberOfThreads);
      final CountDownLatch flagStartRace = new CountDownLatch(1);

      class ThreadOpen extends Thread
      {
         volatile int errors = 0;

         public void run()
         {

            try
            {
               flagAlignSemaphore.countDown();
               flagStartRace.await();

               for (int i = 0; i < numberOfOpens; i++)
               {
                  if (i % 1000 == 0) System.out.println("tests " + i);
                  Connection conn = cf1.createConnection();
                  Session sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
                  sess.close();
                  conn.close();
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               errors++;
            }
         }
      }


      ThreadOpen[] threads = new ThreadOpen[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         threads[i] = new ThreadOpen();
         threads[i].start();
      }

      flagAlignSemaphore.await();
      flagStartRace.countDown();


      for (ThreadOpen t : threads)
      {
         // 5 minutes seems long but this may take a bit of time in a slower box
         t.join(300000);
         assertFalse(t.isAlive());
         assertEquals("There are Errors on the test thread", 0, t.errors);
      }
   }
}
