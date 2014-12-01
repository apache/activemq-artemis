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
package org.apache.activemq.jms.client;

import java.io.Serializable;
import java.util.UUID;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.jms.referenceable.DestinationObjectFactory;
import org.apache.activemq.jms.referenceable.SerializableObjectRefAddr;

/**
 * ActiveMQ implementation of a JMS Destination.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ActiveMQDestination implements Destination, Serializable, Referenceable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    *
    */
   private static final long serialVersionUID = 5027962425462382883L;

   public static final String JMS_QUEUE_ADDRESS_PREFIX = "jms.queue.";

   public static final String JMS_TEMP_QUEUE_ADDRESS_PREFIX = "jms.tempqueue.";

   public static final String JMS_TOPIC_ADDRESS_PREFIX = "jms.topic.";

   public static final String JMS_TEMP_TOPIC_ADDRESS_PREFIX = "jms.temptopic.";

   private static final char SEPARATOR = '.';

   private static String escape(final String input)
   {
      if (input == null)
      {
         return "";
      }
      return input.replace("\\", "\\\\").replace(".", "\\.");
   }

   public static Destination fromAddress(final String address)
   {
      if (address.startsWith(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX))
      {
         String name = address.substring(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX.length());

         return createQueue(name);
      }
      else if (address.startsWith(ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX))
      {
         String name = address.substring(ActiveMQDestination.JMS_TOPIC_ADDRESS_PREFIX.length());

         return createTopic(name);
      }
      else if (address.startsWith(ActiveMQDestination.JMS_TEMP_QUEUE_ADDRESS_PREFIX))
      {
         String name = address.substring(ActiveMQDestination.JMS_TEMP_QUEUE_ADDRESS_PREFIX.length());

         return new ActiveMQTemporaryQueue(address, name, null);
      }
      else if (address.startsWith(ActiveMQDestination.JMS_TEMP_TOPIC_ADDRESS_PREFIX))
      {
         String name = address.substring(ActiveMQDestination.JMS_TEMP_TOPIC_ADDRESS_PREFIX.length());

         return new ActiveMQTemporaryTopic(address, name, null);
      }
      else
      {
         throw new JMSRuntimeException("Invalid address " + address);
      }
   }

   public static String createQueueNameForDurableSubscription(final boolean isDurable, final String clientID, final String subscriptionName)
   {
      if (clientID != null)
      {
         if (isDurable)
         {
            return ActiveMQDestination.escape(clientID) + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         }
         else
         {
            return "nonDurable" + SEPARATOR +
               ActiveMQDestination.escape(clientID) + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         }
      }
      else
      {
         if (isDurable)
         {
            return ActiveMQDestination.escape(subscriptionName);
         }
         else
         {
            return "nonDurable" + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         }
      }
   }

   public static String createQueueNameForSharedSubscription(final boolean isDurable, final String clientID, final String subscriptionName)
   {
      if (clientID != null)
      {
         return (isDurable ? "Durable" : "nonDurable") + SEPARATOR +
            ActiveMQDestination.escape(clientID) + SEPARATOR +
            ActiveMQDestination.escape(subscriptionName);
      }
      else
      {
         return (isDurable ? "Durable" : "nonDurable") + SEPARATOR +
            ActiveMQDestination.escape(subscriptionName);
      }
   }

   public static Pair<String, String> decomposeQueueNameForDurableSubscription(final String queueName)
   {
      StringBuffer[] parts = new StringBuffer[2];
      int currentPart = 0;

      parts[0] = new StringBuffer();
      parts[1] = new StringBuffer();

      int pos = 0;
      while (pos < queueName.length())
      {
         char ch = queueName.charAt(pos);
         pos++;

         if (ch == SEPARATOR)
         {
            currentPart++;
            if (currentPart >= parts.length)
            {
               throw new JMSRuntimeException("Invalid message queue name: " + queueName);
            }

            continue;
         }

         if (ch == '\\')
         {
            if (pos >= queueName.length())
            {
               throw new JMSRuntimeException("Invalid message queue name: " + queueName);
            }
            ch = queueName.charAt(pos);
            pos++;
         }

         parts[currentPart].append(ch);
      }

      if (currentPart != 1)
      {
         throw new JMSRuntimeException("Invalid message queue name: " + queueName);
      }

      Pair<String, String> pair = new Pair<String, String>(parts[0].toString(), parts[1].toString());

      return pair;
   }

   public static SimpleString createQueueAddressFromName(final String name)
   {
      return new SimpleString(JMS_QUEUE_ADDRESS_PREFIX + name);
   }

   public static SimpleString createTopicAddressFromName(final String name)
   {
      return new SimpleString(JMS_TOPIC_ADDRESS_PREFIX + name);
   }

   public static ActiveMQQueue createQueue(final String name)
   {
      return new ActiveMQQueue(name);
   }

   public static ActiveMQTopic createTopic(final String name)
   {
      return new ActiveMQTopic(name);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final String name, final ActiveMQSession session)
   {
      return new ActiveMQTemporaryQueue(JMS_TEMP_QUEUE_ADDRESS_PREFIX.concat(name), name, session);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final String name)
   {
      return createTemporaryQueue(name, null);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final ActiveMQSession session)
   {
      String name = UUID.randomUUID().toString();

      return createTemporaryQueue(name, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(final ActiveMQSession session)
   {
      String name = UUID.randomUUID().toString();

      return createTemporaryTopic(name, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(String name, final ActiveMQSession session)
   {
      return new ActiveMQTemporaryTopic(JMS_TEMP_TOPIC_ADDRESS_PREFIX.concat(name), name, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(String name)
   {
      return createTemporaryTopic(name, null);
   }

   // Attributes ----------------------------------------------------

   /**
    * The JMS name
    */
   protected final String name;

   /**
    * The core address
    */
   private final String address;

   /**
    * SimpleString version of address
    */
   private final SimpleString simpleAddress;

   private final boolean temporary;

   private final boolean queue;

   private final transient ActiveMQSession session;

   // Constructors --------------------------------------------------

   protected ActiveMQDestination(final String address, final String name,
                                 final boolean temporary,
                                 final boolean queue,
                                 final ActiveMQSession session)
   {
      this.address = address;

      this.name = name;

      simpleAddress = new SimpleString(address);

      this.temporary = temporary;

      this.queue = queue;

      this.session = session;
   }

   // Referenceable implementation ---------------------------------------

   public Reference getReference() throws NamingException
   {
      return new Reference(this.getClass().getCanonicalName(),
                           new SerializableObjectRefAddr("ActiveMQ-DEST", this),
                           DestinationObjectFactory.class.getCanonicalName(),
                           null);
   }

   public void delete() throws JMSException
   {
      if (session != null)
      {
         if (session.getCoreSession().isClosed())
         {
            // Temporary queues will be deleted when the connection is closed.. nothing to be done then!
            return;
         }
         if (queue)
         {
            session.deleteTemporaryQueue(this);
         }
         else
         {
            session.deleteTemporaryTopic(this);
         }
      }
   }

   public boolean isQueue()
   {
      return queue;
   }

   // Public --------------------------------------------------------

   public String getAddress()
   {
      return address;
   }

   public SimpleString getSimpleAddress()
   {
      return simpleAddress;
   }

   public String getName()
   {
      return name;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   @Override
   public boolean equals(final Object o)
   {
      if (this == o)
      {
         return true;
      }

      if (!(o instanceof ActiveMQDestination))
      {
         return false;
      }

      ActiveMQDestination that = (ActiveMQDestination)o;

      return address.equals(that.address);
   }

   @Override
   public int hashCode()
   {
      return address.hashCode();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
