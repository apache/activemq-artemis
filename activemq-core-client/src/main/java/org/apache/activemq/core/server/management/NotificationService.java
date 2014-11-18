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
package org.apache.activemq.core.server.management;

/**
 * A NotificationService
 *
 * @author jmesnil
 */
public interface NotificationService
{
   /**
    * the message corresponding to a notification will always contain the properties:
    * <ul>
    * <li><code>ManagementHelper.HDR_NOTIFICATION_TYPE</code> - the type of notification (SimpleString)</li>
    * <li><code>ManagementHelper.HDR_NOTIFICATION_MESSAGE</code> - a message contextual to the notification (SimpleString)</li>
    * <li><code>ManagementHelper.HDR_NOTIFICATION_TIMESTAMP</code> - the timestamp when the notification occurred (long)</li>
    * </ul>
    * in addition to the properties defined in <code>props</code>
    *
    * @see ManagementHelper
    */
   void sendNotification(Notification notification) throws Exception;

   void enableNotifications(boolean enable);

   void addNotificationListener(NotificationListener listener);

   void removeNotificationListener(NotificationListener listener);

}
