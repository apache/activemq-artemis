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

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;

public class SessionCreateConsumerMessage extends QueueAbstractPacket {

   private long id;

   private SimpleString filterString;

   private int priority;

   private boolean browseOnly;

   private boolean requiresResponse;

   public SessionCreateConsumerMessage(final long id,
                                       final SimpleString queueName,
                                       final SimpleString filterString,
                                       final int priority,
                                       final boolean browseOnly,
                                       final boolean requiresResponse) {
      super(SESS_CREATECONSUMER);

      this.id = id;
      this.queueName = queueName;
      this.filterString = filterString;
      this.priority = priority;
      this.browseOnly = browseOnly;
      this.requiresResponse = requiresResponse;
   }

   public SessionCreateConsumerMessage() {
      super(SESS_CREATECONSUMER);
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", id=" + id);
      buff.append(", browseOnly=" + browseOnly);
      buff.append(", requiresResponse=" + requiresResponse);
      buff.append(", priority=" + priority);
      buff.append("]");
      return buff.toString();
   }

   public long getID() {
      return id;
   }

   public SimpleString getFilterString() {
      return filterString;
   }

   public int getPriority() {
      return priority;
   }

   public boolean isBrowseOnly() {
      return browseOnly;
   }

   @Override
   public boolean isRequiresResponse() {
      return requiresResponse;
   }

   public void setQueueName(SimpleString queueName) {
      this.queueName = queueName;
   }

   public void setFilterString(SimpleString filterString) {
      this.filterString = filterString;
   }

   public void setPriority(byte priority) {
      this.priority = priority;
   }

   public void setBrowseOnly(boolean browseOnly) {
      this.browseOnly = browseOnly;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer, final CoreRemotingConnection coreRemotingConnection) {
      buffer.writeLong(id);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(browseOnly);
      buffer.writeBoolean(requiresResponse);
      //Priority Support added in 2.7.0
      if (coreRemotingConnection.isVersionSupportConsumerPriority()) {
         buffer.writeInt(priority);
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      id = buffer.readLong();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      browseOnly = buffer.readBoolean();
      requiresResponse = buffer.readBoolean();
      //Priority Support Added in 2.7.0
      if (buffer.readableBytes() > 0) {
         priority = buffer.readInt();
      } else {
         priority = ActiveMQDefaultConfiguration.getDefaultConsumerPriority();
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (browseOnly ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + (int) (id ^ (id >>> 32));
      result = prime * result + priority;
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionCreateConsumerMessage))
         return false;
      SessionCreateConsumerMessage other = (SessionCreateConsumerMessage) obj;
      if (browseOnly != other.browseOnly)
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (priority != other.priority)
         return false;
      if (id != other.id)
         return false;
      if (queueName == null) {
         if (other.queueName != null)
            return false;
      } else if (!queueName.equals(other.queueName))
         return false;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }
}
