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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

/**
 * A SessionAddMetaDataMessage
 *
 * Packet deprecated: It exists only to support old formats
 */
public class SessionAddMetaDataMessage extends PacketImpl {

   private String key;
   private String data;

   public SessionAddMetaDataMessage() {
      super(SESS_ADD_METADATA);
   }

   public SessionAddMetaDataMessage(String k, String d) {
      this();
      key = k;
      data = d;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeString(key);
      buffer.writeString(data);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      key = buffer.readString();
      data = buffer.readString();
   }

   @Override
   public final boolean isRequiresConfirmations() {
      return false;
   }

   public String getKey() {
      return key;
   }

   public String getData() {
      return data;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((data == null) ? 0 : data.hashCode());
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", key=" + key);
      buff.append(", data=" + data);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionAddMetaDataMessage))
         return false;
      SessionAddMetaDataMessage other = (SessionAddMetaDataMessage) obj;
      if (data == null) {
         if (other.data != null)
            return false;
      } else if (!data.equals(other.data))
         return false;
      if (key == null) {
         if (other.key != null)
            return false;
      } else if (!key.equals(other.key))
         return false;
      return true;
   }

}
