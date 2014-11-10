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

package org.proton.plug.test.minimalserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.ServerSASL;
import org.proton.plug.sasl.AnonymousServerSASL;
import org.proton.plug.sasl.ServerSASLPlain;
import org.proton.plug.util.ByteUtil;
import org.proton.plug.util.DebugInfo;
import org.proton.plug.util.ReusableLatch;

/**
 * @author Clebert Suconic
 */

public class MinimalConnectionSPI implements AMQPConnectionCallback
{
   Channel channel;

   private AMQPConnectionContext connection;

   public MinimalConnectionSPI(Channel channel)
   {
      this.channel = channel;
   }

   ExecutorService executorService = Executors.newSingleThreadExecutor();

   @Override
   public void close()
   {
      executorService.shutdown();
   }

   public void setConnection(AMQPConnectionContext connection)
   {
      this.connection = connection;
   }

   public AMQPConnectionContext getConnection()
   {
      return connection;
   }

   final ReusableLatch latch = new ReusableLatch(0);

   @Override
   public ServerSASL[] getSASLMechnisms()
   {
      return new ServerSASL[]{new AnonymousServerSASL(), new ServerSASLPlain()};
   }

   @Override
   public void onTransport(final ByteBuf bytes, final AMQPConnectionContext connection)
   {
      final int bufferSize = bytes.writerIndex();

      if (DebugInfo.debug)
      {
         // some debug
         byte[] frame = new byte[bytes.writerIndex()];
         int readerOriginalPos = bytes.readerIndex();

         bytes.getBytes(0, frame);

         try
         {
            System.err.println("Buffer Outgoing: " + "\n" + ByteUtil.formatGroup(ByteUtil.bytesToHex(frame), 4, 16));
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         bytes.readerIndex(readerOriginalPos);
      }


      latch.countUp();
      // ^^ debug

      channel.writeAndFlush(bytes).addListener(new ChannelFutureListener()
      {
         @Override
         public void operationComplete(ChannelFuture future) throws Exception
         {
            latch.countDown();

            //   https://issues.apache.org/jira/browse/PROTON-645
//            connection.outputDone(bufferSize);
//            if (connection.capacity() > 0)
//            {
//               channel.read();
//            }
         }
      });

      channel.flush();

      if (connection.isSyncOnFlush())
      {
         try
         {
            if (!latch.await(5, TimeUnit.SECONDS))
            {
               // TODO logs
               System.err.println("Flush took longer than 5 seconds!!!");
            }
         }
         catch (Throwable e)
         {
            e.printStackTrace();
         }
      }
      connection.outputDone(bufferSize);


//      if (connection.capacity() > 0)
//      {
//         channel.read();
//      }
   }

   @Override
   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection)
   {
      return new MinimalSessionSPI();
   }
}
