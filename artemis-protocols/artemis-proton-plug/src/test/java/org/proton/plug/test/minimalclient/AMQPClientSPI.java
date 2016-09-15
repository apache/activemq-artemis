/*
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
package org.proton.plug.test.minimalclient;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.qpid.proton.engine.Connection;
import org.jboss.logging.Logger;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.SASLResult;
import org.proton.plug.ServerSASL;
import org.proton.plug.sasl.AnonymousServerSASL;
import org.proton.plug.sasl.ServerSASLPlain;
import org.proton.plug.util.ByteUtil;
import org.proton.plug.util.ReusableLatch;

public class AMQPClientSPI implements AMQPConnectionCallback {

   private static final Logger log = Logger.getLogger(AMQPClientSPI.class);
   final Channel channel;
   protected AMQPConnectionContext connection;

   public AMQPClientSPI(Channel channel) {
      this.channel = channel;
   }

   @Override
   public void setConnection(AMQPConnectionContext connection) {
      this.connection = connection;
   }

   @Override
   public AMQPConnectionContext getConnection() {
      return connection;
   }

   @Override
   public void close() {

   }

   @Override
   public ServerSASL[] getSASLMechnisms() {
      return new ServerSASL[]{new AnonymousServerSASL(), new ServerSASLPlain()};
   }

   @Override
   public boolean isSupportsAnonymous() {
      return true;
   }

   @Override
   public void sendSASLSupported() {

   }

   @Override
   public boolean validateConnection(Connection connection, SASLResult saslResult) {
      return true;
   }

   final ReusableLatch latch = new ReusableLatch(0);

   @Override
   public void onTransport(final ByteBuf bytes, final AMQPConnectionContext connection) {
      if (log.isTraceEnabled()) {
         ByteUtil.debugFrame(log, "Bytes leaving client", bytes);
      }

      final int bufferSize = bytes.writerIndex();

      latch.countUp();

      channel.writeAndFlush(bytes).addListener(new ChannelFutureListener() {
         @Override
         public void operationComplete(ChannelFuture future) throws Exception {
            //
            //            connection.outputDone(bufferSize);
            latch.countDown();
         }
      });

      if (connection.isSyncOnFlush()) {
         try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
               log.debug("Flush took longer than 5 seconds!!!");
            }
         }
         catch (Throwable e) {
            log.warn(e.getMessage(), e);
         }
      }

      connection.outputDone(bufferSize);

   }

   @Override
   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
      return null;
   }
}
