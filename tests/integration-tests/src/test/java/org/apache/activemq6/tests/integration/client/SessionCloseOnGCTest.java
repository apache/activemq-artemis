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
package org.apache.activemq6.tests.integration.client;

import java.lang.ref.WeakReference;

import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A SessionCloseOnGCTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <mailto:tim.fox@jboss.org">Tim Fox</a>
 */
public class SessionCloseOnGCTest extends ServiceTestBase
{
   private HornetQServer server;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);
      server.start();

      locator = createInVMNonHALocator();
   }

   /**
    * Make sure Sessions are not leaking after closed..
    * Also... we want to make sure the SessionFactory will close itself when there are not references into it
    */
   @Test
   public void testValidateFactoryGC1() throws Exception
   {
      ClientSessionFactory factory = locator.createSessionFactory();

      ClientSession s1 = factory.createSession();
      ClientSession s2 = factory.createSession();

      s1.close();
      s2.close();

      WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
      WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

      s1 = null;
      s2 = null;

      locator.close();

      locator = null;
      UnitTestCase.checkWeakReferences(wrs1, wrs2);

      WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

      factory.close();

      factory = null;

      UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
   }

   @Test
   public void testValidateFactoryGC2() throws Exception
   {
      locator.setUseGlobalPools(false);

      ClientSessionFactory factory = locator.createSessionFactory();

      ClientSession s1 = factory.createSession();
      ClientSession s2 = factory.createSession();

      s1.close();
      s2.close();

      WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
      WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

      s1 = null;
      s2 = null;

      locator.close();

      locator = null;
      UnitTestCase.checkWeakReferences(wrs1, wrs2);

      WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

      factory.close();

      factory = null;

      UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
   }

   @Test
   public void testValidateFactoryGC3() throws Exception
   {
      ClientSessionFactory factory = locator.createSessionFactory();

      ClientSession s1 = factory.createSession();
      ClientSession s2 = factory.createSession();

      s1.close();
      s2.close();

      WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
      WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

      s1 = null;
      s2 = null;

      locator.close();

      locator = null;
      UnitTestCase.checkWeakReferences(wrs1, wrs2);

      WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

      factory = null;

      UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
   }

   @Test
   public void testValidateFactoryGC4() throws Exception
   {
      ClientSessionFactory factory = locator.createSessionFactory();

      ClientSession s1 = factory.createSession();
      ClientSession s2 = factory.createSession();

      WeakReference<ClientSession> wrs1 = new WeakReference<ClientSession>(s1);
      WeakReference<ClientSession> wrs2 = new WeakReference<ClientSession>(s2);

      s1 = null;
      s2 = null;

      locator.close();

      locator = null;
      UnitTestCase.checkWeakReferences(wrs1, wrs2);

      WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

      factory = null;

      UnitTestCase.checkWeakReferences(fref, wrs1, wrs2);
   }

   @Test
   public void testValidateFactoryGC5() throws Exception
   {
      ClientSessionFactory factory = locator.createSessionFactory();

      WeakReference<ClientSessionFactory> fref = new WeakReference<ClientSessionFactory>(factory);

      factory = null;

      locator.close();

      locator = null;
      UnitTestCase.checkWeakReferences(fref);
   }

   @Test
   public void testCloseOneSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl) locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      WeakReference<ClientSession> wses = new WeakReference<ClientSession>(session);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      session = null;

      UnitTestCase.checkWeakReferences(wses);

      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(1, sf.numConnections());
      Assert.assertEquals(1, server.getRemotingService().getConnections().size());
   }

   @Test
   public void testCloseSeveralSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl) locator.createSessionFactory();

      ClientSession session1 = sf.createSession(false, true, true);
      ClientSession session2 = sf.createSession(false, true, true);
      ClientSession session3 = sf.createSession(false, true, true);

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      WeakReference<ClientSession> ref1 = new WeakReference<ClientSession>(session1);
      WeakReference<ClientSession> ref2 = new WeakReference<ClientSession>(session2);
      WeakReference<ClientSession> ref3 = new WeakReference<ClientSession>(session3);

      session1 = null;
      session2 = null;
      session3 = null;

      UnitTestCase.checkWeakReferences(ref1, ref2, ref3);

      int count = 0;
      final int TOTAL_SLEEP_TIME = 400;
      final int MAX_COUNT = 20;
      while (count++ < MAX_COUNT)
      {
         /*
          * The assertion is vulnerable to races, both in the session closing as well as the return
          * value of the sessions.size() (i.e. HashSet.size()).
          */
         synchronized (this)
         {
            // synchronized block will (as a side effect) force sync all field values
            if (sf.numSessions() == 0)
               break;
            Thread.sleep(TOTAL_SLEEP_TIME / MAX_COUNT);
         }
      }
      Assert.assertEquals("# sessions", 0, sf.numSessions());
      Assert.assertEquals("# connections", 1, sf.numConnections());
      Assert.assertEquals("# connections in remoting service", 1, server.getRemotingService().getConnections().size());
   }

}
