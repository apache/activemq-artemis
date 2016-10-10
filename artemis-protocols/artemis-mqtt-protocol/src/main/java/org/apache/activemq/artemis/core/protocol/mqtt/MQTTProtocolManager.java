/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.spi.core.protocol.AbstractProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

/**
 * MQTTProtocolManager
 */
class MQTTProtocolManager extends AbstractProtocolManager<MqttMessage, MQTTInterceptor, MQTTConnection> implements NotificationListener {

   private static final List<String> websocketRegistryNames = Arrays.asList("mqtt", "mqttv3.1");

   private ActiveMQServer server;

   private MQTTLogger log = MQTTLogger.LOGGER;
   private final List<MQTTInterceptor> incomingInterceptors = new ArrayList<>();
   private final List<MQTTInterceptor> outgoingInterceptors = new ArrayList<>();

   MQTTProtocolManager(ActiveMQServer server,
                       List<BaseInterceptor> incomingInterceptors,
                       List<BaseInterceptor> outgoingInterceptors) {
      this.server = server;
      this.updateInterceptors(incomingInterceptors, outgoingInterceptors);
   }

   @Override
   public void onNotification(Notification notification) {
      // TODO handle notifications
   }

   @Override
   public ProtocolManagerFactory getFactory() {
      return new MQTTProtocolManagerFactory();
   }

   @Override
   public void updateInterceptors(List incoming, List outgoing) {
      this.incomingInterceptors.clear();
      this.incomingInterceptors.addAll(getFactory().filterInterceptors(incoming));

      this.outgoingInterceptors.clear();
      this.outgoingInterceptors.addAll(getFactory().filterInterceptors(outgoing));
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection) {
      try {
         MQTTConnection mqttConnection = new MQTTConnection(connection);
         ConnectionEntry entry = new ConnectionEntry(mqttConnection, null, System.currentTimeMillis(), MQTTUtil.DEFAULT_KEEP_ALIVE_FREQUENCY);

         NettyServerConnection nettyConnection = ((NettyServerConnection) connection);
         MQTTProtocolHandler protocolHandler = nettyConnection.getChannel().pipeline().get(MQTTProtocolHandler.class);
         protocolHandler.setConnection(mqttConnection, entry);
         return entry;
      } catch (Exception e) {
         log.error(e);
         return null;
      }
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public void removeHandler(String name) {
      // TODO add support for handlers
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
      connection.bufferReceived(connection.getID(), buffer);
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      pipeline.addLast(MqttEncoder.INSTANCE);
      pipeline.addLast(new MqttDecoder(MQTTUtil.MAX_MESSAGE_SIZE));

      pipeline.addLast(new MQTTProtocolHandler(server, this));
   }

   @Override
   public boolean isProtocol(byte[] array) {
      boolean mqtt311 = array[4] == 77 && // M
         array[5] == 81 && // Q
         array[6] == 84 && // T
         array[7] == 84;   // T

      // FIXME The actual protocol name is 'MQIsdp' (However we are only passed the first 4 bytes of the protocol name)
      boolean mqtt31 = array[4] == 77 && // M
         array[5] == 81 && // Q
         array[6] == 73 && // I
         array[7] == 115;   // s
      return mqtt311 || mqtt31;
   }

   @Override
   public MessageConverter getConverter() {
      return null;
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
   }

   @Override
   public List<String> websocketSubprotocolIdentifiers() {
      return websocketRegistryNames;
   }

   public void invokeIncoming(MqttMessage mqttMessage, MQTTConnection connection) {
      super.invokeInterceptors(this.incomingInterceptors, mqttMessage, connection);
   }

   public void invokeOutgoing(MqttMessage mqttMessage, MQTTConnection connection) {
      super.invokeInterceptors(this.outgoingInterceptors, mqttMessage, connection);
   }
}
