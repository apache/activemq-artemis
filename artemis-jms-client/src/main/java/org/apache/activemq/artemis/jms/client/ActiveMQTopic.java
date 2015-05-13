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
package org.apache.activemq.artemis.jms.client;

import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.SimpleString;

/**
 * ActiveMQ Artemis implementation of a JMS Topic.
 * <br>
 * This class can be instantiated directly.
 */
public class ActiveMQTopic extends ActiveMQDestination implements Topic
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7873614001276404156L;
   // Static --------------------------------------------------------

   public static SimpleString createAddressFromName(final String name)
   {
      return new SimpleString(JMS_TOPIC_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ActiveMQTopic(final String name)
   {
      super(JMS_TOPIC_ADDRESS_PREFIX + name, name, false, false, null);
   }


   /**
    * @param address
    * @param name
    * @param temporary
    * @param session
    */
   protected ActiveMQTopic(String address, String name, boolean temporary, ActiveMQSession session)
   {
      super(address, name, temporary, false, session);
   }


   // Topic implementation ------------------------------------------

   public String getTopicName()
   {
      return name;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "ActiveMQTopic[" + name + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
