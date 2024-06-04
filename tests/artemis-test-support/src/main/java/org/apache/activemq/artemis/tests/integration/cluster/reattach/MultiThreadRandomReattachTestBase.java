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
package org.apache.activemq.artemis.tests.integration.cluster.reattach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMRegistry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQBytesMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public abstract class MultiThreadRandomReattachTestBase extends MultiThreadReattachSupportTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


   private static final int RECEIVE_TIMEOUT = 30000;

   private final int LATCH_WAIT = getLatchWait();

   private final int NUM_THREADS = getNumThreads();

   protected static final SimpleString ADDRESS = SimpleString.of("FailoverTestAddress");

   protected ActiveMQServer server;

   protected abstract int getNumIterations();

   @Override
   protected void start() throws Exception {
      Configuration primaryConf = createDefaultInVMConfig();
      server = createServer(false, primaryConf);
      server.getConfiguration().getAddressConfigurations().add(new CoreAddressConfiguration().setName(ADDRESS.toString()).addRoutingType(RoutingType.MULTICAST));
      server.start();
      waitForServerToStart(server);
   }

   protected void setBody(final ClientMessage message) throws Exception {
      // Give each msg a body
      message.getBodyBuffer().writeBytes(new byte[250]);
   }

   protected boolean checkSize(final ClientMessage message) {
      return message.getBodyBuffer().readableBytes() == 250;
   }

   @Test
   public void testA() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestA(sf, threadNum);
         }
      }, NUM_THREADS, false);

   }

   @Test
   public void testB() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestB(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testC() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestC(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testD() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestD(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testE() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestE(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testF() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestF(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testG() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestG(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testH() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestH(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testI() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestI(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testJ() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestJ(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testK() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestK(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   @Test
   public void testL() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestL(sf);
         }
      }, NUM_THREADS, true, 10);
   }

   // public void testM() throws Exception
   // {
   // runTestMultipleThreads(new RunnableT()
   // {
   // public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
   // {
   // doTestM(sf, threadNum);
   // }
   // }, NUM_THREADS);
   // }

   @Test
   public void testN() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestN(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   // Added do replicate HORNETQ-264
   @Test
   public void testO() throws Exception {
      runTestMultipleThreads(new RunnableT() {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception {
            doTestO(sf, threadNum);
         }
      }, NUM_THREADS, false);
   }

   protected ClientSession createAutoCommitSession(final ClientSessionFactory sf) throws Exception {
      ClientSession session = sf.createSession(false, true, true);
      session.addMetaData("someData", RandomUtil.randomString());
      session.addMetaData("someData2", RandomUtil.randomString());
      return session;
   }

   protected ClientSession createTransactionalSession(final ClientSessionFactory sf) throws Exception {
      ClientSession session = sf.createSession(false, false, false);
      session.addMetaData("someData", RandomUtil.randomString());
      session.addMetaData("someData2", RandomUtil.randomString());

      return session;
   }

   protected void doTestA(final ClientSessionFactory sf,
                          final int threadNum,
                          final ClientSession session2) throws Exception {
      SimpleString subName = SimpleString.of("sub" + threadNum);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      session.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientConsumer consumer = session.createConsumer(subName);

      final int numMessages = 100;

      sendMessages(session, producer, numMessages, threadNum);

      session.start();

      MyHandler handler = new MyHandler(threadNum, numMessages);

      consumer.setMessageHandler(handler);

      boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

      if (!ok) {
         throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
      }

      if (handler.failure != null) {
         throw new Exception("Handler failed: " + handler.failure);
      }

      producer.close();

      consumer.close();

      session.deleteQueue(subName);

      session.close();
   }

   protected void doTestA(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = createAutoCommitSession(sf);

         sessConsume.start();

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);

      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      Set<MyHandler> handlers = new HashSet<>();

      for (ClientConsumer consumer : consumers) {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok) {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                   " threadnum " +
                                   threadNum);
         }

         if (handler.failure != null) {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      sessSend.close();

      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestB(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = createAutoCommitSession(sf);

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);
      sessSend.addMetaData("some-data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.start();
      }

      Set<MyHandler> handlers = new HashSet<>();

      for (ClientConsumer consumer : consumers) {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok) {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                   " threadnum " +
                                   threadNum);
         }

         if (handler.failure != null) {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      sessSend.close();

      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));

   }

   protected void doTestC(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("some-data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = createTransactionalSession(sf);

         sessConsume.start();

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);
      sessSend.addMetaData("some-data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      Set<MyHandler> handlers = new HashSet<>();

      for (ClientConsumer consumer : consumers) {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok) {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                   " threadnum " +
                                   threadNum);
         }

         if (handler.failure != null) {
            throw new Exception("Handler failed: " + handler.failure);
         }

         handler.reset();
      }

      for (ClientSession session : sessions) {
         session.rollback();
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         assertTrue(ok);
      }

      for (ClientSession session : sessions) {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestD(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("some-data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + " sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);
         sessConsume.addMetaData("data", RandomUtil.randomString());

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);
      sessSend.addMetaData("some-data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      for (ClientSession session : sessions) {
         session.start();
      }

      Set<MyHandler> handlers = new HashSet<>();

      for (ClientConsumer consumer : consumers) {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok) {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                   " threadnum " +
                                   threadNum);
         }

         if (handler.failure != null) {
            throw new Exception("Handler failed: " + handler.failure);
         }
      }

      handlers.clear();

      // Set handlers to null
      for (ClientConsumer consumer : consumers) {
         consumer.setMessageHandler(null);
      }

      for (ClientSession session : sessions) {
         session.rollback();
      }

      // New handlers
      for (ClientConsumer consumer : consumers) {
         MyHandler handler = new MyHandler(threadNum, numMessages);

         consumer.setMessageHandler(handler);

         handlers.add(handler);
      }

      for (MyHandler handler : handlers) {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok) {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                   " threadnum " +
                                   threadNum);
         }

         if (handler.failure != null) {
            throw new Exception("Handler failed on rollback: " + handler.failure);
         }
      }

      for (ClientSession session : sessions) {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + " sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   // Now with synchronous receive()

   protected void doTestE(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("some-data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);
         sessConsume.addMetaData("some-data", RandomUtil.randomString());

         sessConsume.start();

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);
      sessSend.addMetaData("some-data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      consumeMessages(consumers, numMessages, threadNum);

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestF(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, true, true);
         sessConsume.addMetaData("data", RandomUtil.randomString());

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, true, true);
      sessSend.addMetaData("data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestG(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);
         sessConsume.addMetaData("data", RandomUtil.randomString());

         sessConsume.start();

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);
      sessSend.addMetaData("data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestH(final ClientSessionFactory sf, final int threadNum) throws Exception {
      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("data", RandomUtil.randomString());

      final int numMessages = 100;

      final int numSessions = 10;

      Set<ClientConsumer> consumers = new HashSet<>();
      Set<ClientSession> sessions = new HashSet<>();

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         ClientSession sessConsume = sf.createSession(false, false, false);
         sessConsume.addMetaData("data", RandomUtil.randomString());

         sessConsume.createQueue(QueueConfiguration.of(subName).setAddress(ADDRESS).setDurable(false));

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         consumers.add(consumer);

         sessions.add(sessConsume);
      }

      ClientSession sessSend = sf.createSession(false, false, false);
      sessSend.addMetaData("data", RandomUtil.randomString());

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.rollback();

      sendMessages(sessSend, producer, numMessages, threadNum);

      sessSend.commit();

      for (ClientSession session : sessions) {
         session.start();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.rollback();
      }

      consumeMessages(consumers, numMessages, threadNum);

      for (ClientSession session : sessions) {
         session.commit();
      }

      sessSend.close();
      for (ClientSession session : sessions) {
         session.close();
      }

      for (int i = 0; i < numSessions; i++) {
         SimpleString subName = SimpleString.of(threadNum + "sub" + i);

         s.deleteQueue(subName);
      }

      s.close();

      long end = System.currentTimeMillis();

      logger.debug("duration {}", (end - start));
   }

   protected void doTestI(final ClientSessionFactory sf, final int threadNum) throws Exception {
      ClientSession sessCreate = sf.createSession(false, true, true);
      sessCreate.addMetaData("data", RandomUtil.randomString());

      sessCreate.createQueue(QueueConfiguration.of(SimpleString.of(threadNum + ADDRESS.toString())).setAddress(ADDRESS).setDurable(false));

      ClientSession sess = sf.createSession(false, true, true);
      sess.addMetaData("data", RandomUtil.randomString());

      sess.start();

      ClientConsumer consumer = sess.createConsumer(SimpleString.of(threadNum + ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
      producer.send(message);

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(SimpleString.of(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestJ(final ClientSessionFactory sf, final int threadNum) throws Exception {
      ClientSession sessCreate = sf.createSession(false, true, true);
      sessCreate.addMetaData("data", RandomUtil.randomString());

      sessCreate.createQueue(QueueConfiguration.of(SimpleString.of(threadNum + ADDRESS.toString())).setAddress(ADDRESS).setDurable(false));

      ClientSession sess = sf.createSession(false, true, true);
      sess.addMetaData("data", RandomUtil.randomString());

      sess.start();

      ClientConsumer consumer = sess.createConsumer(SimpleString.of(threadNum + ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
      producer.send(message);

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.acknowledge();

      sess.close();

      sessCreate.deleteQueue(SimpleString.of(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestK(final ClientSessionFactory sf, final int threadNum) throws Exception {
      ClientSession s = sf.createSession(false, false, false);
      s.addMetaData("data", RandomUtil.randomString());

      s.createQueue(QueueConfiguration.of(SimpleString.of(threadNum + ADDRESS.toString())).setAddress(ADDRESS).setDurable(false));

      final int numConsumers = 100;

      for (int i = 0; i < numConsumers; i++) {
         ClientConsumer consumer = s.createConsumer(SimpleString.of(threadNum + ADDRESS.toString()));

         consumer.close();
      }

      s.deleteQueue(SimpleString.of(threadNum + ADDRESS.toString()));

      s.close();
   }

   /*
    * This test tests failure during create connection
    */
   protected void doTestL(final ClientSessionFactory sf) throws Exception {
      final int numSessions = 100;

      for (int i = 0; i < numSessions; i++) {
         ClientSession session = sf.createSession(false, false, false);

         session.addMetaData("data", RandomUtil.randomString());

         session.close();
      }
   }

   protected void doTestN(final ClientSessionFactory sf, final int threadNum) throws Exception {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(QueueConfiguration.of(SimpleString.of(threadNum + ADDRESS.toString())).setAddress(ADDRESS).setDurable(false));

      ClientSession sess = sf.createSession(false, true, true);
      sess.addMetaData("data", RandomUtil.randomString());

      sess.stop();

      sess.start();

      sess.stop();

      ClientConsumer consumer = sess.createConsumer(SimpleString.of(threadNum + ADDRESS.toString()));

      ClientProducer producer = sess.createProducer(ADDRESS);

      ClientMessage message = sess.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
      producer.send(message);

      sess.start();

      ClientMessage message2 = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

      assertNotNull(message2);

      message2.acknowledge();

      sess.stop();

      sess.start();

      sess.close();

      sessCreate.deleteQueue(SimpleString.of(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected void doTestO(final ClientSessionFactory sf, final int threadNum) throws Exception {
      ClientSession sessCreate = sf.createSession(false, true, true);

      sessCreate.createQueue(QueueConfiguration.of(SimpleString.of(threadNum + ADDRESS.toString())).setAddress(ADDRESS).setDurable(false));

      ClientSession sess = sf.createSession(false, true, true);

      sess.start();

      ClientConsumer consumer = sess.createConsumer(SimpleString.of(threadNum + ADDRESS.toString()));

      for (int i = 0; i < 100; i++) {
         assertNull(consumer.receiveImmediate());
      }

      sess.close();

      sessCreate.deleteQueue(SimpleString.of(threadNum + ADDRESS.toString()));

      sessCreate.close();
   }

   protected int getLatchWait() {
      return 60000;
   }

   protected int getNumThreads() {
      return 10;
   }


   private void runTestMultipleThreads(final RunnableT runnable,
                                       final int numThreads,
                                       final boolean failOnCreateConnection) throws Exception {
      runTestMultipleThreads(runnable, numThreads, failOnCreateConnection, 1000);
   }

   private void runTestMultipleThreads(final RunnableT runnable,
                                       final int numThreads,
                                       final boolean failOnCreateConnection,
                                       final long failDelay) throws Exception {

      runMultipleThreadsFailoverTest(runnable, numThreads, getNumIterations(), failOnCreateConnection, failDelay);
   }

   /**
    * @return
    */
   @Override
   protected ServerLocator createLocator() throws Exception {
      ServerLocator locator = createInVMNonHALocator().setReconnectAttempts(15).setConfirmationWindowSize(1024 * 1024);
      return locator;
   }

   @Override
   protected void stop() throws Exception {
      ActiveMQTestBase.stopComponent(server);

      System.gc();

      assertEquals(0, InVMRegistry.instance.size());
   }

   private void sendMessages(final ClientSession sessSend,
                             final ClientProducer producer,
                             final int numMessages,
                             final int threadNum) throws Exception {
      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = sessSend.createMessage(ActiveMQBytesMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
         message.putIntProperty(SimpleString.of("threadnum"), threadNum);
         message.putIntProperty(SimpleString.of("count"), i);
         setBody(message);
         producer.send(message);
      }
   }

   private void consumeMessages(final Set<ClientConsumer> consumers,
                                final int numMessages,
                                final int threadNum) throws Exception {
      // We make sure the messages arrive in the order they were sent from a particular producer
      Map<ClientConsumer, Map<Integer, Integer>> counts = new HashMap<>();

      for (int i = 0; i < numMessages; i++) {
         for (ClientConsumer consumer : consumers) {
            Map<Integer, Integer> consumerCounts = counts.get(consumer);

            if (consumerCounts == null) {
               consumerCounts = new HashMap<>();
               counts.put(consumer, consumerCounts);
            }

            ClientMessage msg = consumer.receive(MultiThreadRandomReattachTestBase.RECEIVE_TIMEOUT);

            assertNotNull(msg);

            int tn = (Integer) msg.getObjectProperty(SimpleString.of("threadnum"));
            int cnt = (Integer) msg.getObjectProperty(SimpleString.of("count"));

            Integer c = consumerCounts.get(tn);
            if (c == null) {
               c = cnt;
            }

            if (tn == threadNum && cnt != c) {
               throw new Exception("Invalid count, expected " + tn + ": " + c + " got " + cnt);
            }

            c++;

            // Wrap
            if (c == numMessages) {
               c = 0;
            }

            consumerCounts.put(tn, c);

            msg.acknowledge();
         }
      }
   }


   private class MyHandler implements MessageHandler {

      CountDownLatch latch = new CountDownLatch(1);

      private final Map<Integer, Integer> counts = new HashMap<>();

      volatile String failure;

      final int tn;

      final int numMessages;

      volatile boolean done;

      synchronized void reset() {
         counts.clear();

         done = false;

         failure = null;

         latch = new CountDownLatch(1);
      }

      MyHandler(final int threadNum, final int numMessages) {
         tn = threadNum;

         this.numMessages = numMessages;
      }

      @Override
      public synchronized void onMessage(final ClientMessage message) {
         try {
            message.acknowledge();
         } catch (ActiveMQException me) {
            logger.error("Failed to process", me);
         }

         if (done) {
            return;
         }

         int threadNum = (Integer) message.getObjectProperty(SimpleString.of("threadnum"));
         int cnt = (Integer) message.getObjectProperty(SimpleString.of("count"));

         Integer c = counts.get(threadNum);
         if (c == null) {
            c = cnt;
         }

         if (tn == threadNum && cnt != c) {
            failure = "Invalid count, expected " + threadNum + ":" + c + " got " + cnt;
            logger.error(failure);

            latch.countDown();
         }

         if (!checkSize(message)) {
            failure = "Invalid size on message";
            logger.error(failure);
            latch.countDown();
         }

         if (tn == threadNum && c == numMessages - 1) {
            done = true;
            latch.countDown();
         }

         c++;
         // Wrap around at numMessages
         if (c == numMessages) {
            c = 0;
         }

         counts.put(threadNum, c);

      }
   }
}
