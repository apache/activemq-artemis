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
package org.apache.activemq.artemis.core.protocol.openwire;

import java.io.IOException;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

public class OpenWireMessageSupport {

   public static final SimpleString JMS_TYPE_PROPERTY = new SimpleString("JMSType");
   public static final SimpleString JMS_CORRELATION_ID_PROPERTY = new SimpleString("JMSCorrelationID");
   public static final SimpleString AMQ_PREFIX = new SimpleString("__HDR_");

   public static final SimpleString AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = new SimpleString(AMQ_PREFIX + "dlqDeliveryFailureCause");
   public static final SimpleString AMQ_MSG_ARRIVAL = new SimpleString(AMQ_PREFIX + "ARRIVAL");
   public static final SimpleString AMQ_MSG_BROKER_IN_TIME = new SimpleString(AMQ_PREFIX + "BROKER_IN_TIME");
   public static final SimpleString AMQ_MSG_BROKER_PATH = new SimpleString(AMQ_PREFIX + "BROKER_PATH");
   public static final SimpleString AMQ_MSG_CLUSTER = new SimpleString(AMQ_PREFIX + "CLUSTER");
   public static final SimpleString AMQ_MSG_COMMAND_ID = new SimpleString(AMQ_PREFIX + "COMMAND_ID");
   public static final SimpleString AMQ_MSG_DATASTRUCTURE = new SimpleString(AMQ_PREFIX + "DATASTRUCTURE");
   public static final SimpleString AMQ_MSG_MESSAGE_ID = new SimpleString(AMQ_PREFIX + "MESSAGE_ID");
   public static final SimpleString AMQ_MSG_ORIG_DESTINATION =  new SimpleString(AMQ_PREFIX + "ORIG_DESTINATION");
   public static final SimpleString AMQ_MSG_ORIG_TXID = new SimpleString(AMQ_PREFIX + "ORIG_TXID");
   public static final SimpleString AMQ_MSG_PRODUCER_ID =  new SimpleString(AMQ_PREFIX + "PRODUCER_ID");
   public static final SimpleString AMQ_MSG_MARSHALL_PROP = new SimpleString(AMQ_PREFIX + "MARSHALL_PROP");
   public static final SimpleString AMQ_MSG_REPLY_TO = new SimpleString(AMQ_PREFIX + "REPLY_TO");
   public static final SimpleString AMQ_MSG_USER_ID = new SimpleString(AMQ_PREFIX + "USER_ID");
   public static final SimpleString AMQ_MSG_DROPPABLE =  new SimpleString(AMQ_PREFIX + "DROPPABLE");
   public static final SimpleString AMQ_MSG_COMPRESSED = new SimpleString(AMQ_PREFIX + "COMPRESSED");

   public static Object getBinaryObjectProperty(WireFormat format,
                                                ICoreMessage coreMessage,
                                                SimpleString propertyKey) throws IOException {
      if (format == null || coreMessage == null || isNullOrEmpty(propertyKey)) {
         return null;
      }
      Object object = coreMessage.getObjectProperty(propertyKey);
      if (object == null || !(object instanceof byte[])) {
         return null;
      }
      ByteSequence seq = new ByteSequence((byte[]) object);
      return format.unmarshal(seq);
   }

   public static void setBinaryObjectProperty(WireFormat format,
                                              ICoreMessage coreMessage,
                                              SimpleString propertyKey,
                                              Object object) throws IOException {
      if (format == null || coreMessage == null || isNullOrEmpty(propertyKey) || object == null) {
         return;
      }
      ByteSequence objectBytes = format.marshal(object);
      objectBytes.compact();
      coreMessage.putObjectProperty(propertyKey, objectBytes.data);
   }

   public static boolean isNullOrEmpty(SimpleString str) {
      return isNullOrEmpty(str.toString());
   }

   public static boolean isNullOrEmpty(String str) {
      if (str != null && !str.isEmpty()) {
         return false;
      }
      return true;
   }

}
