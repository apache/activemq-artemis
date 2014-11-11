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
package org.apache.activemq6.tests.integration.cluster.failover;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.Interceptor;
import org.apache.activemq6.core.protocol.core.Packet;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;
import org.apache.activemq6.spi.core.protocol.RemotingConnection;

/**
 * A DelayInterceptor
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DelayInterceptor implements Interceptor
{
   public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.SESS_SEND)
      {
         // Lose the send
         return false;
      }
      else
      {
         return true;
      }
   }
}
