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
package org.apache.activemq6.tests.unit.core.remoting.impl.netty;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQBuffers;
import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq6.core.server.HornetQComponent;
import org.apache.activemq6.spi.core.remoting.Connection;
import org.apache.activemq6.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq6.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A NettyConnectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NettyConnectionTest extends UnitTestCase
{
   private static final Map<String, Object> emptyMap = Collections.emptyMap();

   @Test
   public void testGetID() throws Exception
   {
      Channel channel = createChannel();
      NettyConnection conn = new NettyConnection(emptyMap, channel, new MyListener(), false, false);

      Assert.assertEquals(channel.hashCode(), conn.getID());
   }

   @Test
   public void testWrite() throws Exception
   {
      HornetQBuffer buff = HornetQBuffers.wrappedBuffer(ByteBuffer.allocate(128));
      EmbeddedChannel channel = createChannel();

      Assert.assertEquals(0, channel.outboundMessages().size());

      NettyConnection conn = new NettyConnection(emptyMap, channel, new MyListener(), false, false);
      conn.write(buff);
      channel.runPendingTasks();
      Assert.assertEquals(1, channel.outboundMessages().size());
   }

   @Test
   public void testCreateBuffer() throws Exception
   {
      EmbeddedChannel channel = createChannel();
      NettyConnection conn = new NettyConnection(emptyMap, channel, new MyListener(), false, false);

      final int size = 1234;

      HornetQBuffer buff = conn.createBuffer(size);
      buff.writeByte((byte) 0x00); // Netty buffer does lazy initialization.
      Assert.assertEquals(size, buff.capacity());

   }

   private static EmbeddedChannel createChannel()
   {
      return new EmbeddedChannel(new ChannelInboundHandlerAdapter());
   }

   class MyListener implements ConnectionLifeCycleListener
   {

      public void connectionCreated(final HornetQComponent component, final Connection connection, final String protocol)
      {

      }

      public void connectionDestroyed(final Object connectionID)
      {

      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {

      }

      public void connectionReadyForWrites(Object connectionID, boolean ready)
      {
      }

   }
}
