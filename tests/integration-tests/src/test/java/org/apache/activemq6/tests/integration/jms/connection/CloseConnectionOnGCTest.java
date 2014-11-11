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
package org.apache.activemq6.tests.integration.jms.connection;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Session;

import org.junit.Assert;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.jms.HornetQJMSClient;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.core.remoting.CloseListener;
import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.spi.core.protocol.RemotingConnection;
import org.apache.activemq6.tests.util.JMSTestBase;
import org.apache.activemq6.tests.util.UnitTestCase;

/**
 *
 * A CloseConnectionOnGCTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CloseConnectionOnGCTest extends JMSTestBase
{
   private HornetQConnectionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.apache.activemq6.core.remoting.impl.invm.InVMConnectorFactory"));
      cf.setBlockOnDurableSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (cf != null)
         cf.close();

      super.tearDown();
   }

   @Test
   public void testCloseOneConnectionOnGC() throws Exception
   {
      // Debug - don't remove this until intermittent failure with this test is fixed
      int initialConns = server.getRemotingService().getConnections().size();

      Assert.assertEquals(0, initialConns);

      Connection conn = cf.createConnection();

      WeakReference<Connection> wr = new WeakReference<Connection>(conn);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());
      final CountDownLatch latch = new CountDownLatch(1);
      Iterator<RemotingConnection> connectionIterator = server.getRemotingService().getConnections().iterator();
      connectionIterator.next().addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            latch.countDown();
         }
      });

      conn = null;

      UnitTestCase.checkWeakReferences(wr);

      latch.await(5000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(0, server.getRemotingService().getConnections().size());
   }

   @Test
   public void testCloseSeveralConnectionOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();

      WeakReference<Connection> wr1 = new WeakReference<Connection>(conn1);
      WeakReference<Connection> wr2 = new WeakReference<Connection>(conn2);
      WeakReference<Connection> wr3 = new WeakReference<Connection>(conn3);

      Assert.assertEquals(3, server.getRemotingService().getConnections().size());

      final CountDownLatch latch = new CountDownLatch(3);
      Iterator<RemotingConnection> connectionIterator = server.getRemotingService().getConnections().iterator();
      while (connectionIterator.hasNext())
      {
         RemotingConnection remotingConnection = connectionIterator.next();
         remotingConnection.addCloseListener(new CloseListener()
         {
            public void connectionClosed()
            {
               latch.countDown();
            }
         });
      }

      conn1 = null;
      conn2 = null;
      conn3 = null;

      UnitTestCase.checkWeakReferences(wr1, wr2, wr3);

      latch.await(5000, TimeUnit.MILLISECONDS);

      Assert.assertEquals(0, server.getRemotingService().getConnections().size());
   }

   @Test
   public void testCloseSeveralConnectionsWithSessionsOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();

      WeakReference<Connection> wr1 = new WeakReference<Connection>(conn1);
      WeakReference<Connection> wr2 = new WeakReference<Connection>(conn2);
      WeakReference<Connection> wr3 = new WeakReference<Connection>(conn3);

      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess4 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess5 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess6 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess7 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final CountDownLatch latch = new CountDownLatch(3);
      Iterator<RemotingConnection> connectionIterator = server.getRemotingService().getConnections().iterator();
      while (connectionIterator.hasNext())
      {
         RemotingConnection remotingConnection = connectionIterator.next();
         remotingConnection.addCloseListener(new CloseListener()
         {
            public void connectionClosed()
            {
               latch.countDown();
            }
         });
      }
      sess1 = sess2 = sess3 = sess4 = sess5 = sess6 = sess7 = null;

      conn1 = null;
      conn2 = null;
      conn3 = null;

      UnitTestCase.checkWeakReferences(wr1, wr2, wr3);

      latch.await(5000, TimeUnit.MILLISECONDS);

      Assert.assertEquals(0, server.getRemotingService().getConnections().size());
   }
}
