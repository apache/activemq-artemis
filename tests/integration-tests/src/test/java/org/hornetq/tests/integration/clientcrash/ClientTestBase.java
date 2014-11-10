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
package org.hornetq.tests.integration.clientcrash;
import org.junit.Before;

import org.junit.Assert;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ClientTestBase
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public abstract class ClientTestBase extends ServiceTestBase
{

   protected HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(true)
         .setSecurityEnabled(false);
      server = createServer(false, config);
      server.start();
   }

   protected void assertActiveConnections(final int expectedActiveConnections) throws Exception
   {
      assertActiveConnections(expectedActiveConnections, 0);
   }

   protected void assertActiveConnections(final int expectedActiveConnections, long timeout) throws Exception
   {
      timeout += System.currentTimeMillis();
      while (timeout > System.currentTimeMillis() && server.getHornetQServerControl().getConnectionCount() != expectedActiveConnections)
      {
         Thread.sleep(100);
      }
      Assert.assertEquals(expectedActiveConnections, server.getHornetQServerControl().getConnectionCount());
   }

   protected void assertActiveSession(final int expectedActiveSession) throws Exception
   {
      assertActiveSession(expectedActiveSession, 0);
   }

   protected void assertActiveSession(final int expectedActiveSession, long timeout) throws Exception
   {
      timeout += System.currentTimeMillis();
      while (timeout > System.currentTimeMillis() && server.getSessions().size() != expectedActiveSession)
      {
         Thread.sleep(100);
      }
      Assert.assertEquals(expectedActiveSession, server.getSessions().size());
   }


}
