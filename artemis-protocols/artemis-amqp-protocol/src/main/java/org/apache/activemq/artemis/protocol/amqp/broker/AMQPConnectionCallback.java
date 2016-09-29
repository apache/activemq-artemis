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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExtCapability;
import org.apache.activemq.artemis.protocol.amqp.sasl.AnonymousServerSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASL;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.jboss.logging.Logger;

public class AMQPConnectionCallback implements FailureListener, CloseListener {

   private static final Logger logger = Logger.getLogger(AMQPConnectionCallback.class);

   private ConcurrentMap<XidImpl, Transaction> transactions = new ConcurrentHashMap<>();

   private final ProtonProtocolManager manager;

   private final Connection connection;

   protected ActiveMQProtonRemotingConnection protonConnectionDelegate;

   protected AMQPConnectionContext amqpConnection;

   private final ReusableLatch latch = new ReusableLatch(0);

   private final Executor closeExecutor;

   private String remoteContainerId;

   private AtomicBoolean registeredConnectionId = new AtomicBoolean(false);

   private ActiveMQServer server;

   public AMQPConnectionCallback(ProtonProtocolManager manager,
                                 Connection connection,
                                 Executor closeExecutor,
                                 ActiveMQServer server) {
      this.manager = manager;
      this.connection = connection;
      this.closeExecutor = closeExecutor;
      this.server = server;
   }

   public ServerSASL[] getSASLMechnisms() {

      ServerSASL[] result;

      if (isSupportsAnonymous()) {
         result = new ServerSASL[]{new PlainSASL(manager.getServer().getSecurityStore()), new AnonymousServerSASL()};
      } else {
         result = new ServerSASL[]{new PlainSASL(manager.getServer().getSecurityStore())};
      }

      return result;
   }

   public boolean isSupportsAnonymous() {
      boolean supportsAnonymous = false;
      try {
         manager.getServer().getSecurityStore().authenticate(null, null, null);
         supportsAnonymous = true;
      } catch (Exception e) {
         // authentication failed so no anonymous support
      }
      return supportsAnonymous;
   }

   public void close() {
      try {
         if (registeredConnectionId.getAndSet(false)) {
            server.removeClientConnection(remoteContainerId);
         }
         connection.close();
         amqpConnection.close();
      } finally {
         for (Transaction tx : transactions.values()) {
            try {
               tx.rollback();
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            }
         }
      }
   }

   public Executor getExeuctor() {
      if (protonConnectionDelegate != null) {
         return protonConnectionDelegate.getExecutor();
      } else {
         return null;
      }
   }

   public void setConnection(AMQPConnectionContext connection) {
      this.amqpConnection = connection;
   }

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
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      amqpConnection.outputDone(size);
   }

   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
      return new AMQPSessionCallback(this, manager, connection, this.connection, closeExecutor);
   }

   public void sendSASLSupported() {
      connection.write(ActiveMQBuffers.wrappedBuffer(new byte[]{'A', 'M', 'Q', 'P', 3, 1, 0, 0}));
   }

   public boolean validateConnection(org.apache.qpid.proton.engine.Connection connection, SASLResult saslResult) {
      remoteContainerId = connection.getRemoteContainer();
      boolean idOK = server.addClientConnection(remoteContainerId, ExtCapability.needUniqueConnection(connection));
      if (!idOK) {
         //https://issues.apache.org/jira/browse/ARTEMIS-728
         Map<Symbol, Object> connProp = new HashMap<>();
         connProp.put(AmqpSupport.CONNECTION_OPEN_FAILED, "true");
         connection.setProperties(connProp);
         connection.getCondition().setCondition(AmqpError.INVALID_FIELD);
         Map<Symbol, Symbol> info = new HashMap<>();
         info.put(AmqpSupport.INVALID_FIELD, AmqpSupport.CONTAINER_ID);
         connection.getCondition().setInfo(info);
         return false;
      }
      registeredConnectionId.set(true);
      return true;
   }

   @Override
   public void connectionClosed() {
      close();
   }

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver) {
      close();
   }

   @Override
   public void connectionFailed(ActiveMQException exception, boolean failedOver, String scaleDownTargetNodeID) {
      close();
   }

   public Binary newTransaction() {
      XidImpl xid = newXID();
      Transaction transaction = new TransactionImpl(xid, server.getStorageManager(), -1);
      transactions.put(xid, transaction);
      return new Binary(xid.getGlobalTransactionId());
   }

   public Transaction getTransaction(Binary txid) throws ActiveMQAMQPException {
      XidImpl xid = newXID(txid.getArray());
      Transaction tx = transactions.get(xid);

      if (tx == null) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.txNotFound(xid.toString());
      }

      return tx;
   }

   public void removeTransaction(Binary txid) {
      XidImpl xid = newXID(txid.getArray());
      transactions.remove(xid);
   }

   protected XidImpl newXID() {
      return newXID(UUIDGenerator.getInstance().generateStringUUID().getBytes());
   }

   protected XidImpl newXID(byte[] bytes) {
      return new XidImpl("amqp".getBytes(), 1, bytes);
   }

}
