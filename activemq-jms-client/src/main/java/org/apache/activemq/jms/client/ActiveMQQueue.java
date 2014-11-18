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
package org.apache.activemq.jms.client;

import javax.jms.Queue;

import org.apache.activemq.api.core.SimpleString;

/**
 * ActiveMQ implementation of a JMS Queue.
 * <br>
 * This class can be instantiated directly.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 8737 $</tt>
 *
 */
public class ActiveMQQueue extends ActiveMQDestination implements Queue
{
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = -1106092883162295462L;

   // Static --------------------------------------------------------

   public static SimpleString createAddressFromName(final String name)
   {
      return new SimpleString(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ActiveMQQueue(final String name)
   {
      super(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX + name, name, false, true, null);
   }



   /**
    * @param address
    * @param name
    * @param temporary
    * @param session
    */
   public ActiveMQQueue(String address, String name, boolean temporary, ActiveMQSession session)
   {
      super(address, name, temporary, true, session);
   }

   public ActiveMQQueue(final String address, final String name)
   {
      super(address, name, false, true, null);
   }

   // Queue implementation ------------------------------------------

   // Public --------------------------------------------------------

   public String getQueueName()
   {
      return name;
   }

   @Override
   public String toString()
   {
      return "ActiveMQQueue[" + name + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
