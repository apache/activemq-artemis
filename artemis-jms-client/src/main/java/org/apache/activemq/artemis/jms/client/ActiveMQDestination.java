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
package org.apache.activemq.artemis.jms.client;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.QueueAttributes;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.jndi.JNDIStorable;
import org.apache.activemq.artemis.api.core.ParameterisedAddress;

/**
 * ActiveMQ Artemis implementation of a JMS Destination.
 */
public class ActiveMQDestination extends JNDIStorable implements Destination, Serializable {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   private static final long serialVersionUID = 5027962425462382883L;

   public static final String QUEUE_QUALIFIED_PREFIX = "queue://";
   public static final String TOPIC_QUALIFIED_PREFIX = "topic://";
   public static final String TEMP_QUEUE_QUALIFED_PREFIX = "temp-queue://";
   public static final String TEMP_TOPIC_QUALIFED_PREFIX = "temp-topic://";

   private static final char SEPARATOR = '.';

   private static String escape(final String input) {
      if (input == null) {
         return "";
      }
      return input.replace("\\", "\\\\").replace(".", "\\.");
   }

   /**
    * Static helper method for working with destinations.
    */
   public static ActiveMQDestination createDestination(String name, TYPE defaultType) {
      if (name.startsWith(QUEUE_QUALIFIED_PREFIX)) {
         return new ActiveMQQueue(name.substring(QUEUE_QUALIFIED_PREFIX.length()));
      } else if (name.startsWith(TOPIC_QUALIFIED_PREFIX)) {
         return new ActiveMQTopic(name.substring(TOPIC_QUALIFIED_PREFIX.length()));
      } else if (name.startsWith(TEMP_QUEUE_QUALIFED_PREFIX)) {
         return new ActiveMQQueue(name.substring(TEMP_QUEUE_QUALIFED_PREFIX.length()), true);
      } else if (name.startsWith(TEMP_TOPIC_QUALIFED_PREFIX)) {
         return new ActiveMQTopic(name.substring(TEMP_TOPIC_QUALIFED_PREFIX.length()), true);
      }

      switch (defaultType) {
         case QUEUE:
            return new ActiveMQQueue(name);
         case TOPIC:
            return new ActiveMQTopic(name);
         case TEMP_QUEUE:
            return new ActiveMQQueue(name, true);
         case TEMP_TOPIC:
            return new ActiveMQTopic(name, true);
         case DESTINATION:
            return new ActiveMQDestination(name, TYPE.DESTINATION, null);
         default:
            throw new IllegalArgumentException("Invalid default destination type: " + defaultType);
      }
   }

   public static Destination fromPrefixedName(final String name) {
      if (name.startsWith(ActiveMQDestination.QUEUE_QUALIFIED_PREFIX)) {
         String address = name.substring(ActiveMQDestination.QUEUE_QUALIFIED_PREFIX.length());
         return createQueue(address);
      } else if (name.startsWith(ActiveMQDestination.TOPIC_QUALIFIED_PREFIX)) {
         String address = name.substring(ActiveMQDestination.TOPIC_QUALIFIED_PREFIX.length());
         return createTopic(address);
      } else if (name.startsWith(ActiveMQDestination.TEMP_QUEUE_QUALIFED_PREFIX)) {
         String address = name.substring(ActiveMQDestination.TEMP_QUEUE_QUALIFED_PREFIX.length());
         return new ActiveMQTemporaryQueue(address, null);
      } else if (name.startsWith(ActiveMQDestination.TEMP_TOPIC_QUALIFED_PREFIX)) {
         String address = name.substring(ActiveMQDestination.TEMP_TOPIC_QUALIFED_PREFIX.length());
         return new ActiveMQTemporaryTopic(address, null);
      } else {
         return new ActiveMQDestination(name, TYPE.DESTINATION, null);
      }
   }

   public static SimpleString createQueueNameForSubscription(final boolean isDurable,
                                                       final String clientID,
                                                       final String subscriptionName) {
      final String queueName;
      if (clientID != null) {
         if (isDurable) {
            queueName = ActiveMQDestination.escape(clientID) + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         } else {
            queueName = "nonDurable" + SEPARATOR +
               ActiveMQDestination.escape(clientID) + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         }
      } else {
         if (isDurable) {
            queueName = ActiveMQDestination.escape(subscriptionName);
         } else {
            queueName = "nonDurable" + SEPARATOR +
               ActiveMQDestination.escape(subscriptionName);
         }
      }
      return SimpleString.toSimpleString(queueName);
   }

   public static String createQueueNameForSharedSubscription(final boolean isDurable,
                                                             final String clientID,
                                                             final String subscriptionName) {
      if (clientID != null) {
         return (isDurable ? "Durable" : "nonDurable") + SEPARATOR +
            ActiveMQDestination.escape(clientID) + SEPARATOR +
            ActiveMQDestination.escape(subscriptionName);
      } else {
         return (isDurable ? "Durable" : "nonDurable") + SEPARATOR +
            ActiveMQDestination.escape(subscriptionName);
      }
   }

   public static Pair<String, String> decomposeQueueNameForDurableSubscription(final String queueName) {
      StringBuffer[] parts = new StringBuffer[2];
      int currentPart = 0;

      parts[0] = new StringBuffer();
      parts[1] = new StringBuffer();

      int pos = 0;
      while (pos < queueName.length()) {
         char ch = queueName.charAt(pos);
         pos++;

         if (ch == SEPARATOR) {
            currentPart++;
            if (currentPart >= parts.length) {
               throw new JMSRuntimeException("Invalid message queue name: " + queueName);
            }

            continue;
         }

         if (ch == '\\') {
            if (pos >= queueName.length()) {
               throw new JMSRuntimeException("Invalid message queue name: " + queueName);
            }
            ch = queueName.charAt(pos);
            pos++;
         }

         parts[currentPart].append(ch);
      }

      if (currentPart != 1) {
         /* JMS 2.0 introduced the ability to create "shared" subscriptions which do not require a clientID.
          * In this case the subscription name will be the same as the queue name, but the above algorithm will put that
          * in the wrong position in the array so we need to move it.
          */
         parts[1] = parts[0];
         parts[0] = new StringBuffer();
      }

      Pair<String, String> pair = new Pair<>(parts[0].toString(), parts[1].toString());

      return pair;
   }

   public static SimpleString createQueueAddressFromName(final String name) {
      return new SimpleString(QUEUE_QUALIFIED_PREFIX + name);
   }

   public static SimpleString createTopicAddressFromName(final String name) {
      return new SimpleString(TOPIC_QUALIFIED_PREFIX + name);
   }

   public static ActiveMQQueue createQueue(final String address) {
      return new ActiveMQQueue(address);
   }

   public static ActiveMQQueue createQueue(final SimpleString address) {
      return new ActiveMQQueue(address);
   }

   public static ActiveMQQueue createQueue(final String address, final String name) {
      return new ActiveMQQueue(address, name);
   }

   public static ActiveMQTopic createTopic(final String address) {
      return new ActiveMQTopic(address);
   }

   public static ActiveMQTopic createTopic(final SimpleString address) {
      return new ActiveMQTopic(address);
   }

   public static ActiveMQTopic createTopic(final String address, final String name) {
      return new ActiveMQTopic(address, name);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final String address, final ActiveMQSession session) {
      return new ActiveMQTemporaryQueue(address, session);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final String address) {
      return createTemporaryQueue(address, null);
   }

   public static ActiveMQTemporaryQueue createTemporaryQueue(final ActiveMQSession session) {
      String address = UUID.randomUUID().toString();

      return createTemporaryQueue(address, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(final ActiveMQSession session) {
      String address = UUID.randomUUID().toString();

      return createTemporaryTopic(address, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(String address, final ActiveMQSession session) {
      return new ActiveMQTemporaryTopic(address, session);
   }

   public static ActiveMQTemporaryTopic createTemporaryTopic(String address) {
      return createTemporaryTopic(address, null);
   }

   // Attributes ----------------------------------------------------

   /**
    * The core address
    */
   private SimpleString simpleAddress;

   /**
    * Queue parameters;
    */
   private QueueAttributes queueAttributes;

   /**
    * Needed for serialization backwards compatibility.
    */
   @Deprecated
   private String address;

   /**
    * The "JMS" name of the destination. Needed for serialization backwards compatibility.
    */
   @Deprecated
   private String name;

   private final boolean temporary;

   private final boolean queue;

   private transient TYPE thetype;

   private final transient ActiveMQSession session;

   // Constructors --------------------------------------------------

   protected ActiveMQDestination(final String address,
                                 final TYPE type,
                                 final ActiveMQSession session) {
      this(SimpleString.toSimpleString(address), type, session);
   }

   protected ActiveMQDestination(final SimpleString address,
                                 final TYPE type,
                                 final ActiveMQSession session) {

      if (address != null) {
         setSimpleAddress(address);
      }

      this.thetype = type;

      this.session = session;

      this.temporary = TYPE.isTemporary(type);

      this.queue = TYPE.isQueue(type);
   }

   @Deprecated
   protected ActiveMQDestination(final String address,
                                 final String name,
                                 final TYPE type,
                                 final ActiveMQSession session) {
      this(SimpleString.toSimpleString(address), name, type, session);
   }

   @Deprecated
   protected ActiveMQDestination(final SimpleString address,
                                 final String name,
                                 final TYPE type,
                                 final ActiveMQSession session) {
      this(address, type, session);

      this.name = name;

      this.address = simpleAddress != null ? simpleAddress.toString() : null;
   }

   public void setAddress(String address) {
      setSimpleAddress(SimpleString.toSimpleString(address));
   }

   public void setSimpleAddress(SimpleString address) {
      if (address == null) {
         throw new IllegalArgumentException("address cannot be null");
      }
      if (ParameterisedAddress.isParameterised(address)) {
         ParameterisedAddress parameteredAddress = new ParameterisedAddress(address);
         this.simpleAddress = parameteredAddress.getAddress();
         this.address = parameteredAddress.getAddress().toString();
         this.queueAttributes = parameteredAddress.getQueueAttributes();
      } else {
         this.simpleAddress = address;
         this.address = address.toString();
         this.queueAttributes = null;
      }
   }

   public void delete() throws JMSException {
      if (session != null) {
         if (session.getCoreSession().isClosed()) {
            // Temporary queues will be deleted when the connection is closed.. nothing to be done then!
            return;
         }
         if (isQueue()) {
            session.deleteTemporaryQueue(this);
         } else {
            session.deleteTemporaryTopic(this);
         }
      }
   }

   public boolean isQueue() {
      return queue;
   }

   // Public --------------------------------------------------------

   public String getAddress() {
      return simpleAddress.toString();
   }

   public SimpleString getSimpleAddress() {
      return simpleAddress;
   }

   public QueueAttributes getQueueAttributes() {
      return queueAttributes;
   }

   public String getName() {
      return name != null ? name : getAddress();
   }

   public boolean isTemporary() {
      return temporary;
   }

   public TYPE getType() {
      if (thetype == null) {
         if (temporary) {
            if (isQueue()) {
               thetype = TYPE.TEMP_QUEUE;
            } else {
               thetype = TYPE.TEMP_TOPIC;
            }
         } else {
            if (isQueue()) {
               thetype = TYPE.QUEUE;
            } else {
               thetype = TYPE.TOPIC;
            }
         }
      }
      return thetype;
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) {
         return true;
      }

      if (!(o instanceof ActiveMQDestination)) {
         return false;
      }

      ActiveMQDestination that = (ActiveMQDestination) o;

      return simpleAddress.equals(that.simpleAddress);
   }

   @Override
   public int hashCode() {
      return simpleAddress.hashCode();
   }

   @Override
   protected void buildFromProperties(Properties props) {
      setAddress(props.getProperty("address"));
   }

   @Override
   protected void populateProperties(Properties props) {
      props.put("address", getAddress());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public enum TYPE {
      QUEUE,
      TOPIC,
      TEMP_QUEUE,
      TEMP_TOPIC,
      DESTINATION; // unknown

      public byte getType() {
         switch (this) {
            case QUEUE:
               return 0;
            case TOPIC:
               return 1;
            case TEMP_QUEUE:
               return 2;
            case TEMP_TOPIC:
               return 3;
            case DESTINATION:
               return 4;
            default:
               return -1;
         }
      }

      public static TYPE getType(byte type) {
         switch (type) {
            case 0:
               return QUEUE;
            case 1:
               return TOPIC;
            case 2:
               return TEMP_QUEUE;
            case 3:
               return TEMP_TOPIC;
            case 4:
               return DESTINATION;
            default:
               return null;
         }
      }

      public static boolean isQueue(TYPE type) {
         boolean result = false;

         if (type != null && (type.equals(QUEUE) || type.equals(TEMP_QUEUE))) {
            result = true;
         }

         return result;
      }

      public static boolean isTemporary(TYPE type) {
         boolean result = false;

         if (type != null && (type.equals(TEMP_TOPIC) || type.equals(TEMP_QUEUE))) {
            result = true;
         }

         return result;
      }
   }
}
