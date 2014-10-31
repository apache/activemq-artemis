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
package org.hornetq.integration.vertx;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.ConfigurationHelper;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.ReplyException;
import org.vertx.java.core.eventbus.ReplyFailure;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;
import org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory;

public class OutgoingVertxEventHandler implements Consumer, ConnectorService
{
   private final String connectorName;

   private final String queueName;

   private final int port;

   private final String host;

   private final int quorumSize;

   private final String haGroup;

   private final String vertxAddress;

   private final boolean publish;

   private final PostOffice postOffice;

   private Queue queue = null;

   private Filter filter = null;

   private EventBus eventBus;

   private PlatformManager platformManager;

   private boolean isStarted = false;

   public OutgoingVertxEventHandler(String connectorName, Map<String, Object> configuration,
            StorageManager storageManager, PostOffice postOffice,
            ScheduledExecutorService scheduledThreadPool)
   {
      this.connectorName = connectorName;
      this.queueName = ConfigurationHelper.getStringProperty(VertxConstants.QUEUE_NAME, null,
               configuration);
      this.postOffice = postOffice;

      this.port = ConfigurationHelper.getIntProperty(VertxConstants.PORT, 0, configuration);
      this.host = ConfigurationHelper.getStringProperty(VertxConstants.HOST, "localhost",
               configuration);
      this.quorumSize = ConfigurationHelper.getIntProperty(VertxConstants.VERTX_QUORUM_SIZE,
               -1, configuration);
      this.haGroup = ConfigurationHelper.getStringProperty(VertxConstants.VERTX_HA_GROUP,
               "hornetq", configuration);
      this.vertxAddress = ConfigurationHelper.getStringProperty(VertxConstants.VERTX_ADDRESS,
               "org.hornetq", configuration);
      this.publish = ConfigurationHelper.getBooleanProperty(VertxConstants.VERTX_PUBLISH, false,
               configuration);
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

      if (this.connectorName == null || this.connectorName.trim().equals(""))
      {
         throw new Exception("invalid connector name: " + this.connectorName);
      }

      if (this.queueName == null || this.queueName.trim().equals(""))
      {
         throw new Exception("invalid queue name: " + queueName);
      }

      SimpleString name = new SimpleString(this.queueName);
      Binding b = this.postOffice.getBinding(name);
      if (b == null)
      {
         throw new Exception(connectorName + ": queue " + queueName + " not found");
      }
      this.queue = (Queue) b.getBindable();
      this.queue.addConsumer(this);

      this.queue.deliverAsync();
      this.isStarted = true;

      HornetQVertxLogger.LOGGER.debug(connectorName + ": started");
   }

   @Override
   public void stop() throws Exception
   {
      if (!this.isStarted)
      {
         return;
      }

      HornetQVertxLogger.LOGGER.debug(connectorName + ": receive shutdown request");

      this.queue.removeConsumer(this);

      this.platformManager.stop();
      System.clearProperty("vertx.clusterManagerFactory");
      this.isStarted = false;
      HornetQVertxLogger.LOGGER.debug(connectorName + ": stopped");
   }

   @Override
   public boolean isStarted()
   {
      return this.isStarted;
   }

   @Override
   public String getName()
   {
      return this.connectorName;
   }

   @Override
   public HandleStatus handle(MessageReference ref) throws Exception
   {
      if (filter != null && !filter.match(ref.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this)
      {
         ref.handled();

         ServerMessage message = ref.getMessage();

         Object vertxMsgBody = null;
         // extract information from message
         Integer type = message.getIntProperty(VertxConstants.VERTX_MESSAGE_TYPE);

         if (type == null)
         {
            // log a warning and default to raw bytes
            HornetQVertxLogger.LOGGER.nonVertxMessage(message);
            type = VertxConstants.TYPE_RAWBYTES;
         }

         // from vertx
         vertxMsgBody = extractMessageBody(message, type);

         if (vertxMsgBody == null)
         {
            return HandleStatus.NO_MATCH;
         }

         // send to bus
         if (!publish)
         {
            eventBus.send(vertxAddress, vertxMsgBody);
         }
         else
         {
            eventBus.publish(vertxAddress, vertxMsgBody);
         }

         queue.acknowledge(ref);

         HornetQVertxLogger.LOGGER.debug(connectorName + ": forwarded to vertx: "
                  + message.getMessageID());
         return HandleStatus.HANDLED;
      }
   }

   private Object extractMessageBody(ServerMessage message, Integer type) throws Exception
   {
      Object vertxMsgBody = null;
      HornetQBuffer bodyBuffer = message.getBodyBuffer();
      switch (type)
      {
         case VertxConstants.TYPE_PING:
         case VertxConstants.TYPE_STRING:
            bodyBuffer.resetReaderIndex();
            vertxMsgBody = bodyBuffer.readString();
            break;
         case VertxConstants.TYPE_BUFFER:
            int len = bodyBuffer.readInt();
            byte[] bytes = new byte[len];
            bodyBuffer.readBytes(bytes);
            vertxMsgBody = new Buffer(bytes);
            break;
         case VertxConstants.TYPE_BOOLEAN:
            vertxMsgBody = bodyBuffer.readBoolean();
            break;
         case VertxConstants.TYPE_BYTEARRAY:
            int length = bodyBuffer.readInt();
            byte[] byteArray = new byte[length];
            bodyBuffer.readBytes(byteArray);
            vertxMsgBody = byteArray;
            break;
         case VertxConstants.TYPE_BYTE:
            vertxMsgBody = bodyBuffer.readByte();
            break;
         case VertxConstants.TYPE_CHARACTER:
            vertxMsgBody = bodyBuffer.readChar();
            break;
         case VertxConstants.TYPE_DOUBLE:
            vertxMsgBody = bodyBuffer.readDouble();
            break;
         case VertxConstants.TYPE_FLOAT:
            vertxMsgBody = bodyBuffer.readFloat();
            break;
         case VertxConstants.TYPE_INT:
            vertxMsgBody = bodyBuffer.readInt();
            break;
         case VertxConstants.TYPE_LONG:
            vertxMsgBody = bodyBuffer.readLong();
            break;
         case VertxConstants.TYPE_SHORT:
            vertxMsgBody = bodyBuffer.readShort();
            break;
         case VertxConstants.TYPE_JSON_OBJECT:
            vertxMsgBody = new JsonObject(bodyBuffer.readString());
            break;
         case VertxConstants.TYPE_JSON_ARRAY:
            vertxMsgBody = new JsonArray(bodyBuffer.readString());
            break;
         case VertxConstants.TYPE_REPLY_FAILURE:
            int failureType = bodyBuffer.readInt();
            int failureCode = bodyBuffer.readInt();
            String errMsg = bodyBuffer.readString();
            vertxMsgBody = new ReplyException(ReplyFailure.fromInt(failureType), failureCode,
                     errMsg);
            break;
         case VertxConstants.TYPE_RAWBYTES:
            int size = bodyBuffer.readableBytes();
            byte[] rawBytes = new byte[size];
            bodyBuffer.readBytes(rawBytes);
            vertxMsgBody = rawBytes;
            break;
         default:
            HornetQVertxLogger.LOGGER.invalidVertxType(type);
            break;
      }
      return vertxMsgBody;
   }

   @Override
   public void proceedDeliver(MessageReference reference) throws Exception
   {
      // no op
   }

   @Override
   public Filter getFilter()
   {
      return this.filter;
   }

   @Override
   public String debug()
   {
      return null;
   }

   @Override
   public String toManagementString()
   {
      return null;
   }

   @Override
   public List<MessageReference> getDeliveringMessages()
   {
      return null;
   }

   @Override
   public void disconnect()
   {
   }

}
