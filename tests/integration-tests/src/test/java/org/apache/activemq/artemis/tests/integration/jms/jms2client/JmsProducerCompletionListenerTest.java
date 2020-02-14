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

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JmsProducerCompletionListenerTest extends JMSTestBase {

   static final int TOTAL_MSGS = 200;

   private JMSContext context;
   private JMSProducer producer;
   private Queue queue;

   private final int confirmationWindowSize;

   @Parameterized.Parameters(name = "confirmationWindowSize={0}")
   public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][]{{-1}, {0}, {10}, {1000}});
   }

   public JmsProducerCompletionListenerTest(int confirmationWindowSize) {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   @Override
   protected void testCaseCfExtraConfig(ConnectionFactoryConfiguration configuration) {
      configuration.setConfirmationWindowSize(confirmationWindowSize);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      context = createContext();
      producer = context.createProducer();
      queue = createQueue(name.getMethodName() + "Queue");
   }

   @Test
   public void testCompletionListener() throws InterruptedException {
      CountingCompletionListener cl = new CountingCompletionListener(TOTAL_MSGS);
      Assert.assertEquals(null, producer.getAsync());
      producer.setAsync(cl);
      Assert.assertEquals(cl, producer.getAsync());
      producer.setAsync(null);
      producer.setAsync(cl);
      JMSConsumer consumer = context.createConsumer(queue);
      sendMessages(context, producer, queue, TOTAL_MSGS);
      receiveMessages(consumer, 0, TOTAL_MSGS, true);
      assertEquals(TOTAL_MSGS, cl.completion.get());
      context.close();
      Assert.assertTrue("completion listener should be called", cl.completionLatch.await(3, TimeUnit.SECONDS));
   }

   @Test
   public void testNullCompletionListener() throws Exception {
      Connection connection = null;
      try {
         connection = cf.createConnection();
         Session session = connection.createSession();
         MessageProducer prod = session.createProducer(queue);
         prod.send(session.createMessage(), null);
         Assert.fail("Didn't get expected exception!");
      } catch (IllegalArgumentException expected) {
         //ok
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test
   public void testInvalidCallFromListener() throws InterruptedException {
      JMSConsumer consumer = context.createConsumer(queue);
      List<InvalidCompletionListener> listeners = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
         InvalidCompletionListener cl = new InvalidCompletionListener(context, i);
         listeners.add(cl);
         producer.setAsync(cl);
         sendMessages(context, producer, queue, 1);
      }
      receiveMessages(consumer, 0, 1, true);
      context.close();
      for (InvalidCompletionListener cl : listeners) {
         Assert.assertTrue(cl.latch.await(1, TimeUnit.SECONDS));
         Assert.assertNotNull(cl.error);
         Assert.assertTrue(cl.error instanceof IllegalStateRuntimeException);
      }
   }

   public static final class InvalidCompletionListener implements CompletionListener {

      private final JMSContext context;
      public final CountDownLatch latch = new CountDownLatch(1);
      private Exception error;
      private final int call;

      /**
       * @param context
       * @param call
       */
      public InvalidCompletionListener(JMSContext context, int call) {
         this.call = call;
         this.context = context;
      }

      @Override
      public void onCompletion(Message message) {
         latch.countDown();
         try {
            switch (call) {
               case 0:
                  context.rollback();
                  break;
               case 1:
                  context.commit();
                  break;
               case 2:
                  context.close();
                  break;
               default:
                  throw new IllegalArgumentException("call code " + call);
            }
         } catch (Exception error1) {
            this.error = error1;
         }
      }

      @Override
      public void onException(Message message, Exception exception) {
         latch.countDown();
         try {
            switch (call) {
               case 0:
                  context.rollback();
                  break;
               case 1:
                  context.commit();
                  break;
               case 2:
                  context.close();
                  break;
               default:
                  throw new IllegalArgumentException("call code " + call);
            }
         } catch (Exception error1) {
            this.error = error1;
         }
      }

   }

   public static final class CountingCompletionListener implements CompletionListener {

      public AtomicInteger completion = new AtomicInteger(0);
      public int error;
      public CountDownLatch completionLatch;
      public Message lastMessage;

      public CountingCompletionListener(int n) {
         completionLatch = new CountDownLatch(n);
      }

      @Override
      public void onCompletion(Message message) {
         completion.incrementAndGet();
         completionLatch.countDown();
         lastMessage = message;
      }

      @Override
      public void onException(Message message, Exception exception) {
         error++;
      }

      @Override
      public String toString() {
         return JmsProducerCompletionListenerTest.class.getSimpleName() + ":" +
            CountingCompletionListener.class.getSimpleName() + ":" + completionLatch;
      }
   }
}
