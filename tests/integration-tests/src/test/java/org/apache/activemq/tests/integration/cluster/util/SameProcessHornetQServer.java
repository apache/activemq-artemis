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
package org.apache.activemq.tests.integration.cluster.util;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.cluster.ClusterManager;
import org.apache.activemq.tests.util.CountDownSessionFailureListener;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A SameProcessHornetQServer
 *
 * @author jmesnil
 */
public class SameProcessHornetQServer implements TestableServer
{
   private final HornetQServer server;

   public SameProcessHornetQServer(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public boolean isActive()
   {
      return server.isActive();
   }

   public void setIdentity(String identity)
   {
      server.setIdentity(identity);
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }

   public void addInterceptor(Interceptor interceptor)
   {
      server.getRemotingService().addIncomingInterceptor(interceptor);
   }

   public void removeInterceptor(Interceptor interceptor)
   {
      server.getRemotingService().removeIncomingInterceptor(interceptor);
   }

   public void start() throws Exception
   {
      server.start();
   }

   public void stop() throws Exception
   {
      server.stop();
   }

   public CountDownLatch crash(ClientSession... sessions) throws Exception
   {
      return crash(true, sessions);
   }

   public CountDownLatch crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      CountDownLatch latch = new CountDownLatch(sessions.length);
      CountDownSessionFailureListener[] listeners = new CountDownSessionFailureListener[sessions.length];
      for (int i = 0; i < sessions.length; i++)
      {
         listeners[i] = new CountDownSessionFailureListener(latch, sessions[i]);
         sessions[i].addFailureListener(listeners[i]);
      }

      ClusterManager clusterManager = server.getClusterManager();
      clusterManager.flushExecutor();
      clusterManager.clear();
      Assert.assertTrue("server should be running!", server.isStarted());
      server.stop(true);

      if (waitFailure)
      {
         // Wait to be informed of failure
         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         Assert.assertTrue("Failed to stop the server! Latch count is " + latch.getCount() + " out of " +
                  sessions.length, ok);
      }

      return latch;
   }

   public HornetQServer getServer()
   {
      return server;
   }
}
