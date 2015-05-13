/**
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
package org.apache.activemq.artemis.core.protocol.proton;

import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.spi.core.protocol.AbstractRemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.proton.plug.AMQPConnectionContext;

/**
 * This is a Server's Connection representation used by ActiveMQ Artemis.
 */
public class ActiveMQProtonRemotingConnection extends AbstractRemotingConnection
{
   private final AMQPConnectionContext amqpConnection;

   private boolean destroyed = false;

   private final ProtonProtocolManager manager;


   public ActiveMQProtonRemotingConnection(ProtonProtocolManager manager, AMQPConnectionContext amqpConnection, Connection transportConnection, Executor executor)
   {
      super(transportConnection, executor);
      this.manager = manager;
      this.amqpConnection = amqpConnection;
   }

   public Executor getExecutor()
   {
      return this.executor;
   }

   public ProtonProtocolManager getManager()
   {
      return manager;
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final ActiveMQException me, String scaleDownTargetNodeID)
   {
      if (destroyed)
      {
         return;
      }

      destroyed = true;

      ActiveMQClientLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());

      // Then call the listeners
      callFailureListeners(me, scaleDownTargetNodeID);

      callClosingListeners();

      internalClose();
   }


   @Override
   public void destroy()
   {
      synchronized (this)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }


      callClosingListeners();

      internalClose();

   }

   @Override
   public boolean isClient()
   {
      return false;
   }

   @Override
   public boolean isDestroyed()
   {
      return destroyed;
   }

   @Override
   public void disconnect(boolean criticalError)
   {
      getTransportConnection().close();
   }

   /**
    * Disconnect the connection, closing all channels
    */
   @Override
   public void disconnect(String scaleDownNodeID, boolean criticalError)
   {
      getTransportConnection().close();
   }

   @Override
   public boolean checkDataReceived()
   {
      return amqpConnection.checkDataReceived();
   }

   @Override
   public void flush()
   {
      amqpConnection.flush();
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer)
   {
      amqpConnection.inputBuffer(buffer.byteBuf());
      super.bufferReceived(connectionID, buffer);
   }

   private void internalClose()
   {
      // We close the underlying transport connection
      getTransportConnection().close();
   }
}
