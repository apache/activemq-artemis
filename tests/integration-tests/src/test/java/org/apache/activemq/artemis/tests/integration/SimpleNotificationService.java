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
package org.apache.activemq.artemis.tests.integration;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.core.server.management.NotificationService;

public class SimpleNotificationService implements NotificationService {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final List<NotificationListener> listeners = new ArrayList<>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // NotificationService implementation ----------------------------

   @Override
   public void addNotificationListener(final NotificationListener listener) {
      listeners.add(listener);
   }

   @Override
   public void enableNotifications(final boolean enable) {
   }

   @Override
   public void removeNotificationListener(final NotificationListener listener) {
      listeners.remove(listener);
   }

   @Override
   public void sendNotification(final Notification notification) throws Exception {
      for (NotificationListener listener : listeners) {
         listener.onNotification(notification);
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static class Listener implements NotificationListener {

      private final List<Notification> notifications = new ArrayList<>();

      @Override
      public void onNotification(final Notification notification) {
         notifications.add(notification);
      }

      public List<Notification> getNotifications() {
         return notifications;
      }

   }
}
