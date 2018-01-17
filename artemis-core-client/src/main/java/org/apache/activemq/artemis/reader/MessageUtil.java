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
package org.apache.activemq.artemis.reader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;

/**
 * static methods intended for import static on JMS like messages.
 *
 * This provides a helper for core message to act some of the JMS functions used by the JMS wrapper
 */
public class MessageUtil {

   public static final SimpleString CORRELATIONID_HEADER_NAME = new SimpleString("JMSCorrelationID");

   public static final SimpleString REPLYTO_HEADER_NAME = new SimpleString("JMSReplyTo");

   public static final SimpleString TYPE_HEADER_NAME = new SimpleString("JMSType");

   public static final SimpleString JMS = new SimpleString("JMS");

   public static final SimpleString JMSX = new SimpleString("JMSX");

   public static final SimpleString JMS_ = new SimpleString("JMS_");

   public static final String JMSXDELIVERYCOUNT = "JMSXDeliveryCount";

   public static final String JMSXGROUPID = "JMSXGroupID";

   public static final String JMSXUSERID = "JMSXUserID";

   public static final SimpleString CONNECTION_ID_PROPERTY_NAME = new SimpleString("__AMQ_CID");

   //   public static ActiveMQBuffer getBodyBuffer(Message message) {
   //      return message.getBodyBuffer();
   //   }

   public static byte[] getJMSCorrelationIDAsBytes(Message message) {
      Object obj = message.getObjectProperty(CORRELATIONID_HEADER_NAME);

      if (obj instanceof byte[]) {
         return (byte[]) obj;
      } else {
         return null;
      }
   }

   public static void setJMSType(Message message, String type) {
      message.putStringProperty(TYPE_HEADER_NAME, new SimpleString(type));
   }

   public static String getJMSType(Message message) {
      SimpleString ss = message.getSimpleStringProperty(TYPE_HEADER_NAME);

      if (ss != null) {
         return ss.toString();
      } else {
         return null;
      }
   }

   public static final void setJMSCorrelationIDAsBytes(Message message,
                                                       final byte[] correlationID) throws ActiveMQException {
      if (correlationID == null || correlationID.length == 0) {
         throw new ActiveMQException("Please specify a non-zero length byte[]");
      }
      message.putBytesProperty(CORRELATIONID_HEADER_NAME, correlationID);
   }

   public static void setJMSCorrelationID(Message message, final String correlationID) {
      if (correlationID == null) {
         message.removeProperty(CORRELATIONID_HEADER_NAME);
      } else {
         message.putStringProperty(CORRELATIONID_HEADER_NAME, new SimpleString(correlationID));
      }
   }

   public static String getJMSCorrelationID(Message message) {
      try {
         return message.getStringProperty(CORRELATIONID_HEADER_NAME);
      } catch (ActiveMQPropertyConversionException e) {
         return null;
      }
   }

   public static SimpleString getJMSReplyTo(Message message) {
      return message.getSimpleStringProperty(REPLYTO_HEADER_NAME);
   }

   public static void setJMSReplyTo(Message message, final String dest) {
      if (dest == null) {
         message.removeProperty(REPLYTO_HEADER_NAME);
      } else {
         message.putStringProperty(REPLYTO_HEADER_NAME, dest);
      }
   }

   public static void setJMSReplyTo(Message message, final SimpleString dest) {
      if (dest == null) {
         message.removeProperty(REPLYTO_HEADER_NAME);
      } else {
         message.putStringProperty(REPLYTO_HEADER_NAME, dest);
      }
   }

   public static void clearProperties(Message message) {
      /**
       * JavaDoc for this method states:
       *    Clears a message's properties.
       *    The message's header fields and body are not cleared.
       *
       * Since the {@code Message.HDR_ROUTING_TYPE} is used for the JMSDestination header it isn't cleared
       */

      List<SimpleString> toRemove = new ArrayList<>();

      for (SimpleString propName : message.getPropertyNames()) {
         if ((!propName.startsWith(JMS) || propName.startsWith(JMSX) ||
            propName.startsWith(JMS_)) && !propName.equals(Message.HDR_ROUTING_TYPE)) {
            toRemove.add(propName);
         }
      }

      for (SimpleString propName : toRemove) {
         message.removeProperty(propName);
      }
   }

   public static Set<String> getPropertyNames(Message message) {
      HashSet<String> set = new HashSet<>();

      for (SimpleString propName : message.getPropertyNames()) {
         if ((!propName.startsWith(JMS) || propName.startsWith(JMSX) ||
            propName.startsWith(JMS_)) && !propName.startsWith(CONNECTION_ID_PROPERTY_NAME) && !propName.equals(Message.HDR_ROUTING_TYPE) &&
            !propName.startsWith(Message.HDR_ROUTE_TO_IDS)) {
            set.add(propName.toString());
         }
      }

      set.add(JMSXDELIVERYCOUNT);

      return set;
   }

   public static boolean propertyExists(Message message, String name) {
      return message.containsProperty(new SimpleString(name)) || name.equals(MessageUtil.JMSXDELIVERYCOUNT) ||
         (MessageUtil.JMSXGROUPID.equals(name) && message.containsProperty(Message.HDR_GROUP_ID)) ||
         (MessageUtil.JMSXUSERID.equals(name) && message.containsProperty(Message.HDR_VALIDATED_USER));
   }
}
