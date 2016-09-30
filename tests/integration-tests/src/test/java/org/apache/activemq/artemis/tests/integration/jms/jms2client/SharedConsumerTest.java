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
package org.apache.activemq.artemis.tests.integration.jms.jms2client;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Topic;
import java.util.Random;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class SharedConsumerTest extends JMSTestBase {

   private JMSContext context;
   private final Random random = new Random();
   private Topic topic1;
   private Topic topic2;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      context = createContext();
      topic1 = createTopic(JmsContextTest.class.getSimpleName() + "Topic1");
      topic2 = createTopic(JmsContextTest.class.getSimpleName() + "Topic2");
   }

   @Test
   public void sharedDurableSubSimpleRoundRobin() throws Exception {
      context = cf.createContext();
      try {
         JMSConsumer con1 = context.createSharedDurableConsumer(topic1, "mySharedCon");
         JMSConsumer con2 = context.createSharedDurableConsumer(topic1, "mySharedCon");
         context.start();
         JMSProducer producer = context.createProducer();
         int numMessages = 10;
         for (int i = 0; i < numMessages; i++) {
            producer.send(topic1, "msg:" + i);
         }

         for (int i = 0; i < numMessages; i += 2) {
            String msg = con1.receiveBody(String.class, 5000);
            System.out.println("msg = " + msg);
            msg = con2.receiveBody(String.class, 5000);
            System.out.println("msg = " + msg);
         }

      } finally {
         context.close();
      }
   }

   @Test
   public void sharedDurableUnsubscribeNewTopic() throws Exception {
      context = cf.createContext();
      try {
         JMSConsumer con1 = context.createSharedDurableConsumer(topic1, "mySharedCon");
         JMSConsumer con2 = context.createSharedDurableConsumer(topic1, "mySharedCon");
         con1.close();
         con2.close();
         context.unsubscribe("mySharedCon");
         con1 = context.createSharedDurableConsumer(topic2, "mySharedCon");
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedNonDurableUnsubscribeDifferentTopic() throws Exception {
      context = cf.createContext();
      try {
         JMSConsumer con1 = context.createSharedConsumer(topic1, "mySharedCon");
         JMSConsumer con2 = context.createSharedConsumer(topic1, "mySharedCon");
         con1.close();
         Binding binding = server.getPostOffice().getBinding(new SimpleString("nonDurable.mySharedCon"));
         assertNotNull(binding);
         con2.close();
         binding = server.getPostOffice().getBinding(new SimpleString("nonDurable.mySharedCon"));
         assertNull(binding);
         con1 = context.createSharedConsumer(topic2, "mySharedCon");
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedNonDurableSubOnDifferentSelector() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedConsumer(topic1, "mySharedCon", "sel = 'sel1'");
         try {
            context.createSharedConsumer(topic1, "mySharedCon", "sel = 'sel2'");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedNonDurableSubOnDifferentSelectorSrcFilterNull() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedConsumer(topic1, "mySharedCon");
         try {
            context.createSharedConsumer(topic1, "mySharedCon", "sel = 'sel2'");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedNonDurableSubOnDifferentSelectorTargetFilterNull() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedConsumer(topic1, "mySharedCon", "sel = 'sel1'");
         try {
            context.createSharedConsumer(topic1, "mySharedCon");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedDurableSubOnDifferentTopic() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedDurableConsumer(topic1, "mySharedCon");
         try {
            context.createSharedDurableConsumer(topic2, "mySharedCon");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedDurableSubOnDifferentSelector() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedDurableConsumer(topic1, "mySharedCon", "sel = 'sel1'");
         try {
            context.createSharedDurableConsumer(topic1, "mySharedCon", "sel = 'sel2'");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedDurableSubOnDifferentSelectorSrcFilterNull() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedDurableConsumer(topic1, "mySharedCon");
         try {
            context.createSharedDurableConsumer(topic1, "mySharedCon", "sel = 'sel2'");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }

   @Test
   public void sharedDurableSubOnDifferentSelectorTargetFilterNull() throws Exception {
      context = cf.createContext();
      try {
         context.createSharedDurableConsumer(topic1, "mySharedCon", "sel = 'sel1'");
         try {
            context.createSharedDurableConsumer(topic1, "mySharedCon");
            fail("expected JMSRuntimeException");
         } catch (JMSRuntimeException jmse) {
            //pass
         } catch (Exception e) {
            fail("threw wrong exception expected JMSRuntimeException got " + e);
         }
      } finally {
         context.close();
      }
   }
}
