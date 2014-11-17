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
package org.apache.activemq.tests.integration.cluster.failover;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.spi.core.protocol.RemotingConnection;

/**
 * A DelayInterceptor2
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DelayInterceptor2 implements Interceptor
{
   private volatile boolean loseResponse = true;

   private final CountDownLatch latch = new CountDownLatch(1);

   public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.NULL_RESPONSE && loseResponse)
      {
         // Lose the response from the commit - only lose the first one

         loseResponse = false;

         latch.countDown();

         return false;
      }
      else
      {
         return true;
      }
   }

   public boolean await() throws InterruptedException
   {
      return latch.await(10, TimeUnit.SECONDS);
   }
}
