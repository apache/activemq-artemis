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
package org.apache.activemq.artemis.integration.vertx;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ConnectorService;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.ReplyException;
import org.vertx.java.core.eventbus.impl.PingMessage;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;
import org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory;

public class IncomingVertxEventHandler implements ConnectorService
{
   private final String connectorName;

   private final String queueName;

   private final int port;

   private final String host;

   private final int quorumSize;

   private final String haGroup;

   private final String vertxAddress;

   private EventBus eventBus;

   private PlatformManager platformManager;

   private EventHandler handler;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private boolean isStarted = false;

   public IncomingVertxEventHandler(String connectorName, Map<String, Object> configuration,
            StorageManager storageManager, PostOffice postOffice,
            ScheduledExecutorService scheduledThreadPool)
   {
      this.connectorName = connectorName;
      this.queueName = ConfigurationHelper.getStringProperty(VertxConstants.QUEUE_NAME, null,
               configuration);

      this.port = ConfigurationHelper.getIntProperty(VertxConstants.PORT, 0, configuration);
      this.host = ConfigurationHelper.getStringProperty(VertxConstants.HOST, "localhost",
               configuration);
      this.quorumSize = ConfigurationHelper.getIntProperty(VertxConstants.VERTX_QUORUM_SIZE,
               -1, configuration);
      this.haGroup = ConfigurationHelper.getStringProperty(VertxConstants.VERTX_HA_GROUP,
               "activemq", configuration);
      this.vertxAddress = ConfigurationHelper.getStringProperty(VertxConstants.VERTX_ADDRESS,
               "org.apache.activemq", configuration);

      this.storageManager = storageManager;
      this.postOffice = postOffice;
   }

   @Override
   public void start() throws Exception
   {
      if (this.isStarted)
      {
         return;
      }
      System.setProperty("vertx.clusterManagerFactory",
               HazelcastClusterManagerFactory.class.getName());
      if (quorumSize != -1)
      {
         platformManager = PlatformLocator.factory.createPlatformManager(port, host, quorumSize, haGroup);
      }
      else
      {
         platformManager = PlatformLocator.factory.createPlatformManager(port, host);
      }

      eventBus = platformManager.vertx().eventBus();

      Binding b = postOffice.getBinding(new SimpleString(queueName));
      if (b == null)
      {
         throw new Exception(connectorName + ": queue " + queueName + " not found");
      }

      handler = new EventHandler();
      eventBus.registerHandler(vertxAddress, handler);

      isStarted = true;
      ActiveMQVertxLogger.LOGGER.debug(connectorName + ": started");
   }

   @Override
   public void stop() throws Exception
   {
      if (!isStarted)
      {
         return;
      }
      eventBus.unregisterHandler(vertxAddress, handler);
      platformManager.stop();
      System.clearProperty("vertx.clusterManagerFactory");
      isStarted = false;
      ActiveMQVertxLogger.LOGGER.debug(connectorName + ": stopped");
   }

   @Override
   public boolean isStarted()
   {
      return isStarted;
   }

   @Override
   public String getName()
   {
      return connectorName;
   }

   private class EventHandler implements Handler<Message<?>>
   {
      @Override
      public void handle(Message<?> message)
      {
         ServerMessage msg = new ServerMessageImpl(storageManager.generateID(),
                  VertxConstants.INITIAL_MESSAGE_BUFFER_SIZE);
         msg.setAddress(new SimpleString(queueName));
         msg.setDurable(true);
         msg.encodeMessageIDToBuffer();

         String replyAddress = message.replyAddress();
         if (replyAddress != null)
         {
            msg.putStringProperty(VertxConstants.VERTX_MESSAGE_REPLYADDRESS, replyAddress);
         }

         // it'd be better that Message expose its type information
         int type = getMessageType(message);

         msg.putIntProperty(VertxConstants.VERTX_MESSAGE_TYPE, type);

         manualEncodeVertxMessageBody(msg.getBodyBuffer(), message.body(), type);

         try
         {
            postOffice.route(msg, null, false);
         }
         catch (Exception e)
         {
            ActiveMQVertxLogger.LOGGER.error("failed to route msg " + msg, e);
         }
      }

      private void manualEncodeVertxMessageBody(ActiveMQBuffer bodyBuffer, Object body, int type)
      {
         switch (type)
         {
            case VertxConstants.TYPE_BOOLEAN:
               bodyBuffer.writeBoolean(((Boolean) body));
               break;
            case VertxConstants.TYPE_BUFFER:
               Buffer buff = (Buffer) body;
               int len = buff.length();
               bodyBuffer.writeInt(len);
               bodyBuffer.writeBytes(((Buffer) body).getBytes());
               break;
            case VertxConstants.TYPE_BYTEARRAY:
               byte[] bytes = (byte[]) body;
               bodyBuffer.writeInt(bytes.length);
               bodyBuffer.writeBytes(bytes);
               break;
            case VertxConstants.TYPE_BYTE:
               bodyBuffer.writeByte((byte) body);
               break;
            case VertxConstants.TYPE_CHARACTER:
               bodyBuffer.writeChar((Character) body);
               break;
            case VertxConstants.TYPE_DOUBLE:
               bodyBuffer.writeDouble((double) body);
               break;
            case VertxConstants.TYPE_FLOAT:
               bodyBuffer.writeFloat((Float) body);
               break;
            case VertxConstants.TYPE_INT:
               bodyBuffer.writeInt((Integer) body);
               break;
            case VertxConstants.TYPE_LONG:
               bodyBuffer.writeLong((Long) body);
               break;
            case VertxConstants.TYPE_SHORT:
               bodyBuffer.writeShort((Short) body);
               break;
            case VertxConstants.TYPE_STRING:
            case VertxConstants.TYPE_PING:
               bodyBuffer.writeString((String) body);
               break;
            case VertxConstants.TYPE_JSON_OBJECT:
               bodyBuffer.writeString(((JsonObject) body).encode());
               break;
            case VertxConstants.TYPE_JSON_ARRAY:
               bodyBuffer.writeString(((JsonArray) body).encode());
               break;
            case VertxConstants.TYPE_REPLY_FAILURE:
               ReplyException except = (ReplyException) body;
               bodyBuffer.writeInt(except.failureType().toInt());
               bodyBuffer.writeInt(except.failureCode());
               bodyBuffer.writeString(except.getMessage());
               break;
            default:
               throw new IllegalArgumentException("Invalid body type: " + type);
         }
      }

      private int getMessageType(Message<?> message)
      {

         Object body = message.body();

         if (message instanceof PingMessage)
         {
            return VertxConstants.TYPE_PING;
         }
         else if (body instanceof Buffer)
         {
            return VertxConstants.TYPE_BUFFER;
         }
         else if (body instanceof Boolean)
         {
            return VertxConstants.TYPE_BOOLEAN;
         }
         else if (body instanceof byte[])
         {
            return VertxConstants.TYPE_BYTEARRAY;
         }
         else if (body instanceof Byte)
         {
            return VertxConstants.TYPE_BYTE;
         }
         else if (body instanceof Character)
         {
            return VertxConstants.TYPE_CHARACTER;
         }
         else if (body instanceof Double)
         {
            return VertxConstants.TYPE_DOUBLE;
         }
         else if (body instanceof Float)
         {
            return VertxConstants.TYPE_FLOAT;
         }
         else if (body instanceof Integer)
         {
            return VertxConstants.TYPE_INT;
         }
         else if (body instanceof Long)
         {
            return VertxConstants.TYPE_LONG;
         }
         else if (body instanceof Short)
         {
            return VertxConstants.TYPE_SHORT;
         }
         else if (body instanceof String)
         {
            return VertxConstants.TYPE_STRING;
         }
         else if (body instanceof JsonArray)
         {
            return VertxConstants.TYPE_JSON_ARRAY;
         }
         else if (body instanceof JsonObject)
         {
            return VertxConstants.TYPE_JSON_OBJECT;
         }
         else if (body instanceof ReplyException)
         {
            return VertxConstants.TYPE_REPLY_FAILURE;
         }

         throw new IllegalArgumentException("Type not supported: " + message);
      }

   }

   @Override
   public String toString()
   {
      return "[IncomingVertxEventHandler(" + connectorName + "), queueName: " + queueName
               + " host: " + host + " port: " + port + " vertxAddress: " + vertxAddress + "]";
   }
}
