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
package org.apache.activemq.artemis.core.protocol.proton.plug;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.protocol.proton.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.core.protocol.proton.ProtonProtocolManager;
import org.apache.activemq.artemis.core.protocol.proton.sasl.ActiveMQPlainSASL;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.ServerSASL;
import org.proton.plug.sasl.AnonymousServerSASL;

public class ActiveMQProtonConnectionCallback implements AMQPConnectionCallback {

   private final ProtonProtocolManager manager;

   private final Connection connection;

   protected ActiveMQProtonRemotingConnection protonConnectionDelegate;

   protected AMQPConnectionContext amqpConnection;

   private final ReusableLatch latch = new ReusableLatch(0);

   public ActiveMQProtonConnectionCallback(ProtonProtocolManager manager, Connection connection) {
      this.manager = manager;
      this.connection = connection;
   }

   @Override
   public ServerSASL[] getSASLMechnisms() {
      return new ServerSASL[]{new AnonymousServerSASL(), new ActiveMQPlainSASL(manager.getServer().getSecurityStore(), manager.getServer().getSecurityManager())};
   }

   @Override
   public void close() {

   }

   public Executor getExeuctor() {
      if (protonConnectionDelegate != null) {
         return protonConnectionDelegate.getExecutor();
      }
      else {
         return null;
      }
   }

   @Override
   public void setConnection(AMQPConnectionContext connection) {
      this.amqpConnection = connection;
   }

   @Override
   public AMQPConnectionContext getConnection() {
      return amqpConnection;
   }

   public ActiveMQProtonRemotingConnection getProtonConnectionDelegate() {
      return protonConnectionDelegate;
   }

   public void setProtonConnectionDelegate(ActiveMQProtonRemotingConnection protonConnectionDelegate) {
      this.protonConnectionDelegate = protonConnectionDelegate;
   }

   public void onTransport(ByteBuf byteBuf, AMQPConnectionContext amqpConnection) {
      final int size = byteBuf.writerIndex();

      latch.countUp();
      connection.write(new ChannelBufferWrapper(byteBuf, true), false, false, new ChannelFutureListener() {
         @Override
         public void operationComplete(ChannelFuture future) throws Exception {
            latch.countDown();
         }
      });

      if (amqpConnection.isSyncOnFlush()) {
         try {
            latch.await(5, TimeUnit.SECONDS);
         }
         catch (Exception e) {
            e.printStackTrace();
         }
      }

      amqpConnection.outputDone(size);
   }

   @Override
   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
      return new ProtonSessionIntegrationCallback(this, manager, connection);
   }

}
