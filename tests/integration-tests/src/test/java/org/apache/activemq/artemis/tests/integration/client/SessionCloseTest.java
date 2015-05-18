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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

public class SessionCloseTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ActiveMQServer server;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCanNotUseAClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      Assert.assertTrue(session.isClosed());

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.createProducer();
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.createConsumer(RandomUtil.randomSimpleString());
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.createQueue(RandomUtil.randomSimpleString(),
                                RandomUtil.randomSimpleString(),
                                RandomUtil.randomBoolean());
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.createTemporaryQueue(RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString());
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.start();
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.stop();
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.commit();
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.rollback();
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.queueQuery(RandomUtil.randomSimpleString());
         }
      });

      ServiceTestBase.expectActiveMQException(ActiveMQExceptionType.OBJECT_CLOSED, new ActiveMQAction()
      {
         public void run() throws ActiveMQException
         {
            session.addressQuery(RandomUtil.randomSimpleString());
         }
      });

   }

   @Test
   public void testCanNotUseXAWithClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(true, false, false);

      session.close();

      Assert.assertTrue(session.isXA());
      Assert.assertTrue(session.isClosed());

      ServiceTestBase.expectXAException(XAException.XA_RETRY, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.commit(RandomUtil.randomXid(), RandomUtil.randomBoolean());
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.end(RandomUtil.randomXid(), XAResource.TMSUCCESS);
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.forget(RandomUtil.randomXid());
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.prepare(RandomUtil.randomXid());
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.recover(XAResource.TMSTARTRSCAN);
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.rollback(RandomUtil.randomXid());
         }
      });

      ServiceTestBase.expectXAException(XAException.XAER_RMERR, new ActiveMQAction()
      {
         public void run() throws XAException
         {
            session.start(RandomUtil.randomXid(), XAResource.TMNOFLAGS);
         }
      });

   }

   @Test
   public void testCloseHierarchy() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientConsumer consumer = session.createConsumer(queue);

      session.close();

      Assert.assertTrue(session.isClosed());
      Assert.assertTrue(producer.isClosed());
      Assert.assertTrue(consumer.isClosed());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig()
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()))
         .setSecurityEnabled(false);
      server = ActiveMQServers.newActiveMQServer(config, false);

      server.start();

      ServerLocator locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (sf != null)
      {
         sf.close();
      }

      if (server != null)
      {
         server.stop();
      }

      sf = null;

      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
