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
package org.hornetq.tests.timing.jms.bridge.impl;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerInvocationHandler;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.bridge.ConnectionFactoryFactory;
import org.hornetq.jms.bridge.DestinationFactory;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.bridge.impl.JMSBridgeImpl;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.UnitTestLogger;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSBridgeImplTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   private static final String SOURCE = RandomUtil.randomString();

   private static final String TARGET = RandomUtil.randomString();

   private JMSServerManager jmsServer;

   // Static --------------------------------------------------------

   protected static TransactionManager newTransactionManager()
   {
      return new TransactionManager()
      {
         public Transaction suspend() throws SystemException
         {
            return null;
         }

         public void setTransactionTimeout(final int arg0) throws SystemException
         {
         }

         public void setRollbackOnly() throws IllegalStateException, SystemException
         {
         }

         public void rollback() throws IllegalStateException, SecurityException, SystemException
         {
         }

         public void resume(final Transaction arg0) throws InvalidTransactionException,
            IllegalStateException,
            SystemException
         {
         }

         public Transaction getTransaction() throws SystemException
         {
            return null;
         }

         public int getStatus() throws SystemException
         {
            return 0;
         }

         public void commit() throws RollbackException,
            HeuristicMixedException,
            HeuristicRollbackException,
            SecurityException,
            IllegalStateException,
            SystemException
         {
         }

         public void begin() throws NotSupportedException, SystemException
         {
         }
      };
   }

   private static DestinationFactory newDestinationFactory(final Destination dest)
   {
      return new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return dest;
         }
      };
   }

   private static ConnectionFactoryFactory newConnectionFactoryFactory(final ConnectionFactory cf)
   {
      return new ConnectionFactoryFactory()
      {
         public ConnectionFactory createConnectionFactory() throws Exception
         {
            return cf;
         }
      };
   }

   private static ConnectionFactory createConnectionFactory()
   {

      HornetQJMSConnectionFactory cf = (HornetQJMSConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName()));
      // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
      cf.setReconnectAttempts(0);
      cf.setBlockOnNonDurableSend(true);
      cf.setBlockOnDurableSend(true);
      return cf;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testStartWithRepeatedFailure() throws Exception
   {
      HornetQJMSConnectionFactory failingSourceCF = new HornetQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()))
      {
         private static final long serialVersionUID = 2834937512213001068L;

         @Override
         public Connection createConnection() throws JMSException
         {
            throw new JMSException("unable to create a conn");
         }
      };

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(failingSourceCF);
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      // retry after 10 ms
      bridge.setFailureRetryInterval(10);
      // retry only once
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();

      Thread.sleep(50);
      Assert.assertFalse(bridge.isStarted());
      Assert.assertTrue(bridge.isFailed());

      bridge.stop();

   }

   @Test
   public void testStartWithFailureThenSuccess() throws Exception
   {
      HornetQJMSConnectionFactory failingSourceCF = new HornetQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()))
      {
         private static final long serialVersionUID = 4657153922210359725L;
         boolean firstTime = true;

         @Override
         public Connection createConnection() throws JMSException
         {
            if (firstTime)
            {
               firstTime = false;
               throw new JMSException("unable to create a conn");
            }
            else
            {
               return super.createConnection();
            }
         }
      };
      // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
      failingSourceCF.setReconnectAttempts(0);
      failingSourceCF.setBlockOnNonDurableSend(true);
      failingSourceCF.setBlockOnDurableSend(true);

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(failingSourceCF);
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      // retry after 10 ms
      bridge.setFailureRetryInterval(10);
      // retry only once
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();

      Thread.sleep(500);
      Assert.assertTrue(bridge.isStarted());
      Assert.assertFalse(bridge.isFailed());

      bridge.stop();
   }

   /*
   * we receive only 1 message. The message is sent when the maxBatchTime
   * expires even if the maxBatchSize is not reached
   */
   @Test
   public void testSendMessagesWhenMaxBatchTimeExpires() throws Exception
   {
      int maxBatchSize = 2;
      long maxBatchTime = 500;

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();
      Assert.assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(maxBatchSize);
      bridge.setMaxBatchTime(maxBatchTime);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();
      Assert.assertTrue(bridge.isStarted());

      Connection targetConn = JMSBridgeImplTest.createConnectionFactory().createConnection();
      Session targetSess = targetConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = targetSess.createConsumer(targetDF.createDestination());
      final List<Message> messages = new LinkedList<Message>();
      MessageListener listener = new MessageListener()
      {

         public void onMessage(final Message message)
         {
            messages.add(message);
         }
      };
      consumer.setMessageListener(listener);
      targetConn.start();

      Connection sourceConn = JMSBridgeImplTest.createConnectionFactory().createConnection();
      Session sourceSess = sourceConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = sourceSess.createProducer(sourceDF.createDestination());
      producer.send(sourceSess.createTextMessage());
      sourceConn.close();

      Assert.assertEquals(0, messages.size());
      Thread.sleep(3 * maxBatchTime);

      Assert.assertEquals(1, messages.size());

      bridge.stop();
      Assert.assertFalse(bridge.isStarted());

      targetConn.close();
   }

   @Test
   public void testSendMessagesWithMaxBatchSize() throws Exception
   {
      final int numMessages = 10;

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();
      Assert.assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(-1);
      bridge.setMaxBatchSize(numMessages);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();
      Assert.assertTrue(bridge.isStarted());

      Connection targetConn = JMSBridgeImplTest.createConnectionFactory().createConnection();
      Session targetSess = targetConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = targetSess.createConsumer(targetDF.createDestination());
      final List<Message> messages = new LinkedList<Message>();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      MessageListener listener = new MessageListener()
      {
         public void onMessage(final Message message)
         {
            messages.add(message);
            latch.countDown();
         }
      };
      consumer.setMessageListener(listener);
      targetConn.start();

      Connection sourceConn = JMSBridgeImplTest.createConnectionFactory().createConnection();
      Session sourceSess = sourceConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer producer = sourceSess.createProducer(sourceDF.createDestination());

      for (int i = 0; i < numMessages - 1; i++)
      {
         TextMessage msg = sourceSess.createTextMessage();
         producer.send(msg);
         JMSBridgeImplTest.log.info("sent message " + i);
      }

      Thread.sleep(1000);

      Assert.assertEquals(0, messages.size());

      TextMessage msg = sourceSess.createTextMessage();

      producer.send(msg);

      Assert.assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));

      sourceConn.close();

      Assert.assertEquals(numMessages, messages.size());

      bridge.stop();
      Assert.assertFalse(bridge.isStarted());

      targetConn.close();
   }

   @Test
   public void testAutoAckOnSourceBatchOfOne() throws Exception
   {
      doTestAutoAckOnSource(1);
   }

   @Test
   public void testAutoAckOnSourceBatchOfTen() throws Exception
   {
      doTestAutoAckOnSource(10);
   }

   public void doTestAutoAckOnSource(int maxBatchSize) throws Exception
   {
      final int numMessages = maxBatchSize;

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();
      Assert.assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(maxBatchSize);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();
      Assert.assertTrue(bridge.isStarted());

      Connection sourceConn = JMSBridgeImplTest.createConnectionFactory().createConnection();
      Session sourceSess = sourceConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer producer = sourceSess.createProducer(sourceDF.createDestination());

      for (int i = 0; i < numMessages; i++)
      {
         TextMessage msg = sourceSess.createTextMessage();
         producer.send(msg);
         JMSBridgeImplTest.log.info("sent message " + i);
      }

      sourceConn.close();

      JMSQueueControl jmsQueueControl = MBeanServerInvocationHandler.newProxyInstance(
         ManagementFactory.getPlatformMBeanServer(),
         ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(JMSBridgeImplTest.SOURCE),
         JMSQueueControl.class,
         false);
      assertNotEquals(jmsQueueControl.getDeliveringCount(), numMessages);

      bridge.stop();
      Assert.assertFalse(bridge.isStarted());
   }

   @Test
   public void testExceptionOnSourceAndRetrySucceeds() throws Exception
   {
      final AtomicReference<Connection> sourceConn = new AtomicReference<Connection>();
      HornetQJMSConnectionFactory failingSourceCF = new HornetQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()))
      {
         private static final long serialVersionUID = -8866390811966688830L;

         @Override
         public Connection createConnection() throws JMSException
         {
            sourceConn.set(super.createConnection());
            return sourceConn.get();
         }
      };
      // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
      failingSourceCF.setReconnectAttempts(0);
      failingSourceCF.setBlockOnNonDurableSend(true);
      failingSourceCF.setBlockOnDurableSend(true);

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(failingSourceCF);
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();
      Assert.assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(10);
      bridge.setMaxRetries(2);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();
      Assert.assertTrue(bridge.isStarted());

      sourceConn.get().getExceptionListener().onException(new JMSException("exception on the source"));
      Thread.sleep(4 * bridge.getFailureRetryInterval());
      // reconnection must have succeeded
      Assert.assertTrue(bridge.isStarted());

      bridge.stop();
      Assert.assertFalse(bridge.isStarted());
   }

   @Test
   public void testExceptionOnSourceAndRetryFails() throws Exception
   {
      final AtomicReference<Connection> sourceConn = new AtomicReference<Connection>();
      HornetQJMSConnectionFactory failingSourceCF =
         new HornetQJMSConnectionFactory(false, new TransportConfiguration(INVM_CONNECTOR_FACTORY))
         {
            private static final long serialVersionUID = 8216804886099984645L;
            boolean firstTime = true;

            @Override
            public Connection createConnection() throws JMSException
            {
               if (firstTime)
               {
                  firstTime = false;
                  sourceConn.set(super.createConnection());
                  return sourceConn.get();
               }
               else
               {
                  throw new JMSException("exception while retrying to connect");
               }
            }
         };
      // Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
      failingSourceCF.setReconnectAttempts(0);
      failingSourceCF.setBlockOnNonDurableSend(true);
      failingSourceCF.setBlockOnDurableSend(true);

      ConnectionFactoryFactory sourceCFF = JMSBridgeImplTest.newConnectionFactoryFactory(failingSourceCF);
      ConnectionFactoryFactory targetCFF = JMSBridgeImplTest.newConnectionFactoryFactory(JMSBridgeImplTest.createConnectionFactory());
      DestinationFactory sourceDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.SOURCE));
      DestinationFactory targetDF = JMSBridgeImplTest.newDestinationFactory(HornetQJMSClient.createQueue(JMSBridgeImplTest.TARGET));
      TransactionManager tm = JMSBridgeImplTest.newTransactionManager();

      JMSBridgeImpl bridge = new JMSBridgeImpl();
      Assert.assertNotNull(bridge);

      bridge.setSourceConnectionFactoryFactory(sourceCFF);
      bridge.setSourceDestinationFactory(sourceDF);
      bridge.setTargetConnectionFactoryFactory(targetCFF);
      bridge.setTargetDestinationFactory(targetDF);
      bridge.setFailureRetryInterval(100);
      bridge.setMaxRetries(1);
      bridge.setMaxBatchSize(1);
      bridge.setMaxBatchTime(-1);
      bridge.setTransactionManager(tm);
      bridge.setQualityOfServiceMode(QualityOfServiceMode.AT_MOST_ONCE);

      Assert.assertFalse(bridge.isStarted());
      bridge.start();
      Assert.assertTrue(bridge.isStarted());

      sourceConn.get().getExceptionListener().onException(new JMSException("exception on the source"));
      Thread.sleep(4 * bridge.getFailureRetryInterval());
      // reconnection must have failed
      Assert.assertFalse(bridge.isStarted());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createBasicConfig()
         .setFileDeploymentEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      InVMNamingContext context = new InVMNamingContext();
      jmsServer = new JMSServerManagerImpl(HornetQServers.newHornetQServer(config, false));
      jmsServer.setContext(context);
      jmsServer.start();

      jmsServer.createQueue(false, JMSBridgeImplTest.SOURCE, null, true, "/queue/" + JMSBridgeImplTest.SOURCE);
      jmsServer.createQueue(false, JMSBridgeImplTest.TARGET, null, true, "/queue/" + JMSBridgeImplTest.TARGET);

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      jmsServer.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
