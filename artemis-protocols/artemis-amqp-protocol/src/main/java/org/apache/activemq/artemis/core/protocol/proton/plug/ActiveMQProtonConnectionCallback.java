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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.protocol.proton.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.core.protocol.proton.ProtonProtocolManager;
import org.apache.activemq.artemis.core.protocol.proton.sasl.ActiveMQPlainSASL;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.jboss.logging.Logger;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.SASLResult;
import org.proton.plug.ServerSASL;
import org.proton.plug.handler.ExtCapability;
import org.proton.plug.sasl.AnonymousServerSASL;
import org.proton.plug.sasl.PlainSASLResult;

import static org.proton.plug.AmqpSupport.CONTAINER_ID;
import static org.proton.plug.AmqpSupport.INVALID_FIELD;
import static org.proton.plug.context.AbstractConnectionContext.CONNECTION_OPEN_FAILED;

public class ActiveMQProtonConnectionCallback implements AMQPConnectionCallback {
   private static final Logger log = Logger.getLogger(ActiveMQProtonConnectionCallback.class);

   private final ProtonProtocolManager manager;

   private final Connection connection;

   protected ActiveMQProtonRemotingConnection protonConnectionDelegate;

   protected AMQPConnectionContext amqpConnection;

   private final ReusableLatch latch = new ReusableLatch(0);

   private final Executor closeExecutor;

   private ServerSession internalSession;

   public ActiveMQProtonConnectionCallback(ProtonProtocolManager manager,
                                           Connection connection,
                                           Executor closeExecutor) {
      this.manager = manager;
      this.connection = connection;
      this.closeExecutor = closeExecutor;
   }

   @Override
   public ServerSASL[] getSASLMechnisms() {

      ServerSASL[] result;

      if (isSupportsAnonymous()) {
         result = new ServerSASL[]{new ActiveMQPlainSASL(manager.getServer().getSecurityStore()), new AnonymousServerSASL()};
      }
      else {
         result = new ServerSASL[]{new ActiveMQPlainSASL(manager.getServer().getSecurityStore())};
      }

      return result;
   }

   @Override
   public boolean isSupportsAnonymous() {
      boolean supportsAnonymous = false;
      try {
         manager.getServer().getSecurityStore().authenticate(null, null, null);
         supportsAnonymous = true;
      }
      catch (Exception e) {
         // authentication failed so no anonymous support
      }
      return supportsAnonymous;
   }

   @Override
   public void init() throws Exception {
      //This internal core session is used to represent the connection
      //in core server. It is used to identify unique clientIDs.
      //Note the Qpid-JMS client does create a initial session
      //for each connection. However is comes in as a Begin
      //After Open. This makes it unusable for this purpose
      //as we need to decide the uniqueness in response to
      //Open, and the checking Uniqueness and adding the unique
      //client-id to server need to be atomic.
      if (internalSession == null) {
         SASLResult saslResult = amqpConnection.getSASLResult();
         String user = null;
         String passcode = null;
         if (saslResult != null) {
            user = saslResult.getUser();
            if (saslResult instanceof PlainSASLResult) {
               passcode = ((PlainSASLResult) saslResult).getPassword();
            }
         }
         internalSession = manager.getServer().createSession(createInternalSessionName(), user, passcode, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, protonConnectionDelegate, // RemotingConnection remotingConnection,
                 false,
                 false,
                 false,
                 false,
                 null, (SessionCallback) createSessionCallback(this.amqpConnection), true);
      }
   }

   @Override
   public void close() {
      try {
         internalSession.close(false);
      }
      catch (Exception e) {
         log.error("error closing internal session", e);
      }
      connection.close();
      amqpConnection.close();
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

   @Override
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
      return new ProtonSessionIntegrationCallback(this, manager, connection, this.connection, closeExecutor);
   }

   @Override
   public void sendSASLSupported() {
      connection.write(ActiveMQBuffers.wrappedBuffer(new byte[]{'A', 'M', 'Q', 'P', 3, 1, 0, 0}));
   }

   @Override
   public boolean validateConnection(org.apache.qpid.proton.engine.Connection connection, SASLResult saslResult) {
      String remote = connection.getRemoteContainer();

      if (ExtCapability.needUniqueConnection(connection)) {
         if (!internalSession.addUniqueMetaData(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY, remote)) {
            //https://issues.apache.org/jira/browse/ARTEMIS-728
            Map<Symbol, Object> connProp = new HashMap<>();
            connProp.put(CONNECTION_OPEN_FAILED, "true");
            connection.setProperties(connProp);
            connection.getCondition().setCondition(AmqpError.INVALID_FIELD);
            Map<Symbol, Symbol> info = new HashMap<>();
            info.put(INVALID_FIELD, CONTAINER_ID);
            connection.getCondition().setInfo(info);
            return false;
         }
      }
      return true;
   }

   private String createInternalSessionName() {
      return "amqp:" + UUIDGenerator.getInstance().generateStringUUID();
   }
}
