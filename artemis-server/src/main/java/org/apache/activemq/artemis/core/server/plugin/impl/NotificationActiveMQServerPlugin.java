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
package org.apache.activemq.artemis.core.server.plugin.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

/**
 *
 */
public class NotificationActiveMQServerPlugin implements ActiveMQServerPlugin {

   private static final Logger logger = Logger.getLogger(NotificationActiveMQServerPlugin.class);

   public static final String SEND_CONNECTION_NOTIFICATIONS = "SEND_CONNECTION_NOTIFICATIONS";
   public static final String SEND_SESSION_NOTIFICATIONS = "SEND_SESSION_NOTIFICATIONS";
   public static final String SEND_ADDRESS_NOTIFICATIONS = "SEND_ADDRESS_NOTIFICATIONS";
   public static final String SEND_DELIVERED_NOTIFICATIONS = "SEND_DELIVERED_NOTIFICATIONS";
   public static final String SEND_EXPIRED_NOTIFICATIONS = "SEND_EXPIRED_NOTIFICATIONS";

   private boolean sendConnectionNotifications;
   private boolean sendSessionNotifications;
   private boolean sendAddressNotifications;
   private boolean sendDeliveredNotifications;
   private boolean sendExpiredNotifications;


   private final AtomicReference<ManagementService> managementService = new AtomicReference<>();

   /**
    * used to pass configured properties to Plugin
    *
    * @param properties
    */
   @Override
   public void init(Map<String, String> properties) {
      sendConnectionNotifications = Boolean.parseBoolean(properties.getOrDefault(SEND_CONNECTION_NOTIFICATIONS,
            Boolean.FALSE.toString()));
      sendSessionNotifications = Boolean.parseBoolean(properties.getOrDefault(SEND_SESSION_NOTIFICATIONS,
            Boolean.FALSE.toString()));
      sendAddressNotifications = Boolean.parseBoolean(properties.getOrDefault(SEND_ADDRESS_NOTIFICATIONS,
            Boolean.FALSE.toString()));
      sendDeliveredNotifications = Boolean.parseBoolean(properties.getOrDefault(SEND_DELIVERED_NOTIFICATIONS,
            Boolean.FALSE.toString()));
      sendExpiredNotifications = Boolean.parseBoolean(properties.getOrDefault(SEND_EXPIRED_NOTIFICATIONS,
            Boolean.FALSE.toString()));
   }

   @Override
   public void registered(ActiveMQServer server) {
      managementService.set(server.getManagementService());
   }

   @Override
   public void unregistered(ActiveMQServer server) {
      managementService.set(null);
   }

   @Override
   public void afterCreateConnection(RemotingConnection connection) throws ActiveMQException {
      sendConnectionNotification(connection, CoreNotificationType.CONNECTION_CREATED);
   }

   @Override
   public void afterDestroyConnection(RemotingConnection connection) throws ActiveMQException {
      sendConnectionNotification(connection, CoreNotificationType.CONNECTION_DESTROYED);
   }

   @Override
   public void afterCreateSession(ServerSession session) throws ActiveMQException {
      sendSessionNotification(session, CoreNotificationType.SESSION_CREATED);
   }

   @Override
   public void afterCloseSession(ServerSession session, boolean failed) throws ActiveMQException {
      sendSessionNotification(session, CoreNotificationType.SESSION_CLOSED);
   }

   @Override
   public void afterAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {
      sendAddressNotification(addressInfo, CoreNotificationType.ADDRESS_ADDED);
   }

   @Override
   public void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      sendAddressNotification(addressInfo, CoreNotificationType.ADDRESS_REMOVED);
   }

   @Override
   public void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      final ManagementService managementService = getManagementService();

      if (managementService != null && sendDeliveredNotifications) {
         try {
            if (!reference.getQueue().getAddress().equals(managementService.getManagementNotificationAddress())) {
               final TypedProperties props = new TypedProperties();
               props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, consumer.getQueueAddress());
               props.putByteProperty(ManagementHelper.HDR_ROUTING_TYPE, consumer.getQueueType().getType());
               props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, consumer.getQueueName());
               props.putLongProperty(ManagementHelper.HDR_CONSUMER_NAME, consumer.getID());
               props.putLongProperty(ManagementHelper.HDR_MESSAGE_ID, reference.getMessageID());

               managementService.sendNotification(new Notification(null, CoreNotificationType.MESSAGE_DELIVERED, props));
            }
         } catch (Exception e) {
            logger.warn("Error sending notification: " + CoreNotificationType.MESSAGE_DELIVERED, e.getMessage(), e);
         }
      }
   }

   @Override
   public void messageExpired(MessageReference message,
                              SimpleString messageExpiryAddress,
                              ServerConsumer consumer) {
      final ManagementService managementService = getManagementService();

      if (managementService != null && sendExpiredNotifications) {
         try {
            if (!message.getQueue().getAddress().equals(managementService.getManagementNotificationAddress())) {
               final TypedProperties props = new TypedProperties();
               props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, message.getQueue().getAddress());
               props.putByteProperty(ManagementHelper.HDR_ROUTING_TYPE, message.getQueue().getRoutingType().getType());
               props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, message.getQueue().getName());
               props.putLongProperty(ManagementHelper.HDR_MESSAGE_ID, message.getMessageID());
               if (message.hasConsumerId()) {
                  props.putLongProperty(ManagementHelper.HDR_CONSUMER_NAME, message.getConsumerId());
               }

               managementService.sendNotification(new Notification(null, CoreNotificationType.MESSAGE_EXPIRED, props));
            }
         } catch (Exception e) {
            logger.warn("Error sending notification: " + CoreNotificationType.MESSAGE_EXPIRED, e.getMessage(), e);
         }
      }
   }

   private void sendAddressNotification(AddressInfo addressInfo, final CoreNotificationType type) {
      final ManagementService managementService = getManagementService();

      if (managementService != null && sendAddressNotifications) {
         try {
            final TypedProperties props = new TypedProperties();
            props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, addressInfo.getName());
            props.putByteProperty(ManagementHelper.HDR_ROUTING_TYPE, addressInfo.getRoutingType().getType());

            managementService.sendNotification(new Notification(null, type, props));
         } catch (Exception e) {
            logger.warn("Error sending notification: " + type, e.getMessage(), e);
         }
      }
   }

   private void sendConnectionNotification(final RemotingConnection connection, final CoreNotificationType type) {
      final ManagementService managementService = getManagementService();

      if (managementService != null && sendConnectionNotifications) {
         try {
            final TypedProperties props = new TypedProperties();
            props.putSimpleStringProperty(ManagementHelper.HDR_CONNECTION_NAME, SimpleString.toSimpleString(connection.getID().toString()));
            props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(connection.getRemoteAddress()));

            managementService.sendNotification(new Notification(null, type, props));
         } catch (Exception e) {
            logger.warn("Error sending notification: " + type, e.getMessage(), e);
         }
      }
   }

   private void sendSessionNotification(final ServerSession session, final CoreNotificationType type) {
      final ManagementService managementService = getManagementService();

      if (managementService != null && sendSessionNotifications) {
         try {
            final TypedProperties props = new TypedProperties();
            props.putSimpleStringProperty(ManagementHelper.HDR_CONNECTION_NAME, SimpleString.toSimpleString(session.getConnectionID().toString()));
            props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(session.getUsername()));
            props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(session.getName()));

            managementService.sendNotification(new Notification(null, type, props));
         } catch (Exception e) {
            logger.warn("Error sending notification: " + type, e.getMessage(), e);
         }
      }
   }

   /**
    * @return the sendConnectionNotifications
    */
   public boolean isSendConnectionNotifications() {
      return sendConnectionNotifications;
   }

   /**
    * @param sendConnectionNotifications the sendConnectionNotifications to set
    */
   public void setSendConnectionNotifications(boolean sendConnectionNotifications) {
      this.sendConnectionNotifications = sendConnectionNotifications;
   }

   /**
    * @return the sendSessionNotifications
    */
   public boolean isSendSessionNotifications() {
      return sendSessionNotifications;
   }

   /**
    * @param sendSessionNotifications the sendSessionNotifications to set
    */
   public void setSendSessionNotifications(boolean sendSessionNotifications) {
      this.sendSessionNotifications = sendSessionNotifications;
   }

   /**
    * @return the sendDeliveredNotifications
    */
   public boolean isSendDeliveredNotifications() {
      return sendDeliveredNotifications;
   }

   /**
    * @param sendDeliveredNotifications the sendDeliveredNotifications to set
    */
   public void setSendDeliveredNotifications(boolean sendDeliveredNotifications) {
      this.sendDeliveredNotifications = sendDeliveredNotifications;
   }

   /**
    * @return the sendExpiredNotifications
    */
   public boolean isSendExpiredNotifications() {
      return sendExpiredNotifications;
   }

   /**
    * @param sendExpiredNotifications the sendExpiredNotifications to set
    */
   public void setSendExpiredNotifications(boolean sendExpiredNotifications) {
      this.sendExpiredNotifications = sendExpiredNotifications;
   }

   /**
    * @return the sendAddressNotifications
    */
   public boolean isSendAddressNotifications() {
      return sendAddressNotifications;
   }

   /**
    * @param sendAddressNotifications the sendAddressNotifications to set
    */
   public void setSendAddressNotifications(boolean sendAddressNotifications) {
      this.sendAddressNotifications = sendAddressNotifications;
   }

   private ManagementService getManagementService() {
      return this.managementService.get();
   }
}
