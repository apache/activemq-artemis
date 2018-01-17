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

import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQRemoteDisconnectException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

import javax.security.auth.Subject;

/**
 * This is a Server's Connection representation used by ActiveMQ Artemis.
 */
public class ActiveMQProtonRemotingConnection extends AbstractRemotingConnection {

   private final AMQPConnectionContext amqpConnection;

   private boolean destroyed = false;

   private final ProtonProtocolManager manager;

   public ActiveMQProtonRemotingConnection(ProtonProtocolManager manager,
                                           AMQPConnectionContext amqpConnection,
                                           Connection transportConnection,
                                           Executor executor) {
      super(transportConnection, executor);
      this.manager = manager;
      this.amqpConnection = amqpConnection;
   }

   public Executor getExecutor() {
      return this.executor;
   }

   public ProtonProtocolManager getManager() {
      return manager;
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   @Override
   public void fail(final ActiveMQException me, String scaleDownTargetNodeID) {
      if (destroyed) {
         return;
      }

      destroyed = true;

      //filter it like the other protocols
      if (!(me instanceof ActiveMQRemoteDisconnectException)) {
         ActiveMQClientLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      }

      // Then call the listeners
      callFailureListeners(me, scaleDownTargetNodeID);

      callClosingListeners();

      internalClose();
   }

   @Override
   public void destroy() {
      synchronized (this) {
         if (destroyed) {
            return;
         }

         destroyed = true;
      }

      callClosingListeners();

      internalClose();

   }

   @Override
   public boolean isClient() {
      return false;
   }

   @Override
   public boolean isDestroyed() {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError) {
      ErrorCondition errorCondition = new ErrorCondition();
      errorCondition.setCondition(AmqpSupport.CONNECTION_FORCED);
      amqpConnection.close(errorCondition);
      getTransportConnection().close();
   }

   /**
    * Disconnect the connection, closing all channels
    */
   @Override
   public void disconnect(String scaleDownNodeID, boolean criticalError) {
      disconnect(criticalError);
   }

   @Override
   public boolean checkDataReceived() {
      return amqpConnection.checkDataReceived();
   }

   @Override
   public void flush() {
      amqpConnection.flush();
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {
      amqpConnection.inputBuffer(buffer.byteBuf());
      super.bufferReceived(connectionID, buffer);
   }

   private void internalClose() {
      // We close the underlying transport connection
      getTransportConnection().close();
   }

   @Override
   public void killMessage(SimpleString nodeID) {
      //unsupported
   }

   @Override
   public Subject getSubject() {
      SASLResult saslResult = amqpConnection.getSASLResult();
      if (saslResult != null) {
         return saslResult.getSubject();
      }
      return null;
   }

   /**
    * Returns the name of the protocol for this Remoting Connection
    *
    * @return
    */
   @Override
   public String getProtocolName() {
      return ProtonProtocolManagerFactory.AMQP_PROTOCOL_NAME;
   }

   @Override
   public String getClientID() {
      return amqpConnection.getRemoteContainer();
   }

   public void open() {
      amqpConnection.open();
   }

   @Override
   public String getTransportLocalAddress() {
      return getTransportConnection().getLocalAddress();
   }

}
