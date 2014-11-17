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
package org.apache.activemq6.jms.client;

import javax.jms.Topic;

import org.apache.activemq6.api.core.SimpleString;

/**
 * HornetQ implementation of a JMS Topic.
 * <br>
 * This class can be instantiated directly.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 8737 $</tt>
 *
 */
public class HornetQTopic extends HornetQDestination implements Topic
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7873614001276404156L;
   // Static --------------------------------------------------------

   public static SimpleString createAddressFromName(final String name)
   {
      return new SimpleString(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQTopic(final String name)
   {
      super(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + name, name, false, false, null);
   }


   /**
    * @param address
    * @param name
    * @param temporary
    * @param session
    */
   protected HornetQTopic(String address, String name, boolean temporary, HornetQSession session)
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
      return "HornetQTopic[" + name + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
