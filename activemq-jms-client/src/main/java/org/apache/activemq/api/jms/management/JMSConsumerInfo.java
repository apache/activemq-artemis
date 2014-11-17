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
package org.apache.activemq6.api.jms.management;

import org.apache.activemq6.utils.json.JSONArray;
import org.apache.activemq6.utils.json.JSONObject;

/**
 * Helper class to create Java Objects from the
 * JSON serialization returned by {@link JMSServerControl#listConsumersAsJSON(String)} and related methods.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSConsumerInfo
{
   private final String consumerID;

   private final String connectionID;

   private final String destinationName;

   private final String destinationType;

   private final boolean browseOnly;

   private final long creationTime;

   private final boolean durable;

   private final String filter;

   // Static --------------------------------------------------------

   /**
    * Returns an array of SubscriptionInfo corresponding to the JSON serialization returned
    * by {@link TopicControl#listAllSubscriptionsAsJSON()} and related methods.
    */
   public static JMSConsumerInfo[] from(final String jsonString) throws Exception
   {
      JSONArray array = new JSONArray(jsonString);
      JMSConsumerInfo[] infos = new JMSConsumerInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject sub = array.getJSONObject(i);
         JMSConsumerInfo info = new JMSConsumerInfo(sub.getString("consumerID"),
                                                    sub.getString("connectionID"),
                                                    sub.getString("destinationName"),
                                                    sub.getString("destinationType"),
                                                    sub.getBoolean("browseOnly"),
                                                    sub.getLong("creationTime"),
                                                    sub.getBoolean("durable"),
                                                    sub.optString("filter", null));
         infos[i] = info;
      }

      return infos;
   }

   // Constructors --------------------------------------------------

   private JMSConsumerInfo(final String consumerID,
                           final String connectionID,
                            final String destinationName,
                            final String destinationType,
                            final boolean browseOnly,
                            final long creationTime,
                            final boolean durable,
                            final String filter)
   {
      this.consumerID = consumerID;
      this.connectionID = connectionID;
      this.destinationName = destinationName;
      this.destinationType = destinationType;
      this.browseOnly = browseOnly;
      this.creationTime = creationTime;
      this.durable = durable;
      this.filter = filter;
   }

   // Public --------------------------------------------------------

   public String getConsumerID()
   {
      return consumerID;
   }

   public String getConnectionID()
   {
      return connectionID;
   }

   public String getDestinationName()
   {
      return destinationName;
   }

   public String getDestinationType()
   {
      return destinationType;
   }

   public boolean isBrowseOnly()
   {
      return browseOnly;
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   /**
    * @return the durable
    */
   public boolean isDurable()
   {
      return durable;
   }

   public String getFilter()
   {
      return filter;
   }
}
