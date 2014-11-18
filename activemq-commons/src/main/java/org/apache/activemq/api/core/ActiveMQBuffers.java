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

package org.apache.activemq.api.core;

import java.nio.ByteBuffer;

import io.netty.buffer.Unpooled;
import org.apache.activemq.core.buffers.impl.ChannelBufferWrapper;

/**
 * Factory class to create instances of {@link ActiveMQBuffer}.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ActiveMQBuffers
{
   /**
    * Creates a <em>self-expanding</em> HornetQBuffer with the given initial size
    *
    * @param size the initial size of the created HornetQBuffer
    * @return a self-expanding HornetQBuffer starting with the given size
    */
   public static ActiveMQBuffer dynamicBuffer(final int size)
   {
      return new ChannelBufferWrapper(Unpooled.buffer(size));
   }

   /**
    * Creates a <em>self-expanding</em> HornetQBuffer filled with the given byte array
    *
    * @param bytes the created buffer will be initially filled with this byte array
    * @return a self-expanding HornetQBuffer filled with the given byte array
    */
   public static ActiveMQBuffer dynamicBuffer(final byte[] bytes)
   {
      ActiveMQBuffer buff = dynamicBuffer(bytes.length);

      buff.writeBytes(bytes);

      return buff;
   }

   /**
    * Creates a HornetQBuffer wrapping an underlying NIO ByteBuffer
    *
    * The position on this buffer won't affect the position on the inner buffer
    *
    * @param underlying the underlying NIO ByteBuffer
    * @return a HornetQBuffer wrapping the underlying NIO ByteBuffer
    */
   public static ActiveMQBuffer wrappedBuffer(final ByteBuffer underlying)
   {
      ActiveMQBuffer buff = new ChannelBufferWrapper(Unpooled.wrappedBuffer(underlying));

      buff.clear();

      return buff;
   }

   /**
    * Creates a HornetQBuffer wrapping an underlying byte array
    *
    * @param underlying the underlying byte array
    * @return a HornetQBuffer wrapping the underlying byte array
    */
   public static ActiveMQBuffer wrappedBuffer(final byte[] underlying)
   {
      return new ChannelBufferWrapper(Unpooled.wrappedBuffer(underlying));
   }

   /**
    * Creates a <em>fixed</em> HornetQBuffer of the given size
    *
    * @param size the size of the created HornetQBuffer
    * @return a fixed HornetQBuffer with the given size
    */
   public static ActiveMQBuffer fixedBuffer(final int size)
   {
      return new ChannelBufferWrapper(Unpooled.buffer(size, size));
   }

   private ActiveMQBuffers()
   {
      // Utility class
   }
}
