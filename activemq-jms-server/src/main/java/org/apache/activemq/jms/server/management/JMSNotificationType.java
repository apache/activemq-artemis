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
package org.apache.activemq.jms.server.management;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.management.NotificationType;

public enum JMSNotificationType implements NotificationType
{
   QUEUE_CREATED(0),
   QUEUE_DESTROYED(1),
   TOPIC_CREATED(2),
   TOPIC_DESTROYED(3),
   CONNECTION_FACTORY_CREATED(4),
   CONNECTION_FACTORY_DESTROYED(5);

   public static final SimpleString MESSAGE = new SimpleString("message");

   private int type;

   private JMSNotificationType(int type)
   {
      this.type = type;
   }

   @Override
   public int getType()
   {
      return type;
   }
}
