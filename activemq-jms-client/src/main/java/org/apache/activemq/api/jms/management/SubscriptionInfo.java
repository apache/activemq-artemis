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
package org.apache.activemq.api.jms.management;

import org.apache.activemq.utils.json.JSONArray;
import org.apache.activemq.utils.json.JSONObject;

/**
 * Helper class to create Java Objects from the
 * JSON serialization returned by {@link TopicControl#listAllSubscriptionsAsJSON()} and related methods.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SubscriptionInfo
{
   private final String queueName;

   private final String clientID;

   private final String name;

   private final boolean durable;

   private final String selector;

   private final int messageCount;

   private final int deliveringCount;

   // Static --------------------------------------------------------

   /**
    * Returns an array of SubscriptionInfo corresponding to the JSON serialization returned
    * by {@link TopicControl#listAllSubscriptionsAsJSON()} and related methods.
    */
   public static SubscriptionInfo[] from(final String jsonString) throws Exception
   {
      JSONArray array = new JSONArray(jsonString);
      SubscriptionInfo[] infos = new SubscriptionInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject sub = array.getJSONObject(i);
         SubscriptionInfo info = new SubscriptionInfo(sub.getString("queueName"),
                                                      sub.optString("clientID", null),
                                                      sub.optString("name", null),
                                                      sub.getBoolean("durable"),
                                                      sub.optString("selector", null),
                                                      sub.getInt("messageCount"),
                                                      sub.getInt("deliveringCount"));
         infos[i] = info;
      }

      return infos;
   }

   // Constructors --------------------------------------------------

   private SubscriptionInfo(final String queueName,
                            final String clientID,
                            final String name,
                            final boolean durable,
                            final String selector,
                            final int messageCount,
                            final int deliveringCount)
   {
      this.queueName = queueName;
      this.clientID = clientID;
      this.name = name;
      this.durable = durable;
      this.selector = selector;
      this.messageCount = messageCount;
      this.deliveringCount = deliveringCount;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the name of the HornetQ core queue corresponding to this subscription.
    */
   public String getQueueName()
   {
      return queueName;
   }

   /**
    * Returns the client ID of this subscription or {@code null}.
    */
   public String getClientID()
   {
      return clientID;
   }

   /**
    * Returns the name of this subscription.
    */
   public String getName()
   {
      return name;
   }

   /**
    * Returns whether this subscription is durable.
    */
   public boolean isDurable()
   {
      return durable;
   }

   /**
    * Returns the JMS message selector associated to this subscription.
    */
   public String getSelector()
   {
      return selector;
   }

   /**
    * Returns the number of messages currently held by this subscription.
    */
   public int getMessageCount()
   {
      return messageCount;
   }

   /**
    * Returns the number of messages currently delivered to this subscription.
    */
   public int getDeliveringCount()
   {
      return deliveringCount;
   }
}
