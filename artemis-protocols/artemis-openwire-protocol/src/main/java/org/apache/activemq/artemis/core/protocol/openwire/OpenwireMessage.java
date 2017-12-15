/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.utils.collections.TypedProperties;

public class OpenwireMessage extends CoreMessage {

   public boolean compressed;
   public long arrival;
   long brokerInTime;
   String broketPath;
   String cluster;
   int commandId;
   byte[] dataStructure;
   byte[] messageId;
   byte[] producerId;
   byte[] marshallProperty;
   byte[] origDestBytes;
   byte[] replyToDestBytes;
   byte[] origTxId;
   int groupSequence;
   boolean droppable;

   public OpenwireMessage() {
      super();
   }

   public OpenwireMessage(CoreMessage other, TypedProperties copyProperties) {
      super(other, copyProperties);
   }

   public OpenwireMessage(CoreMessage other) {
      super(other);
   }

   public OpenwireMessage(long id, int bufferSize) {
      super(id, bufferSize);
   }

   public boolean isCompressed() {
      return compressed;
   }

   public void setCompressed(boolean compressed) {
      this.compressed = compressed;
   }

   public long getArrival() {
      return arrival;
   }

   public void setArrival(long arrival) {
      this.arrival = arrival;
   }

   public long getBrokerInTime() {
      return brokerInTime;
   }

   public void setBrokerInTime(long brokerInTime) {
      this.brokerInTime = brokerInTime;
   }

   public String getBroketPath() {
      return broketPath;
   }

   public void setBroketPath(String broketPath) {
      this.broketPath = broketPath;
   }

   public String getCluster() {
      return cluster;
   }

   public void setCluster(String cluster) {
      this.cluster = cluster;
   }

   public int getCommandId() {
      return commandId;
   }

   public void setCommandId(int commandId) {
      this.commandId = commandId;
   }

   public byte[] getDataStructure() {
      return dataStructure;
   }

   public void setDataStructure(byte[] dataStructure) {
      this.dataStructure = dataStructure;
   }

   public byte[] getMessageId() {
      return messageId;
   }

   public void setMessageId(byte[] messageId) {
      this.messageId = messageId;
   }

   public byte[] getProducerId() {
      return producerId;
   }

   public void setProducerId(byte[] producerId) {
      this.producerId = producerId;
   }

   public byte[] getMarshallProperty() {
      return marshallProperty;
   }

   public void setMarshallProperty(byte[] marshallProperty) {
      this.marshallProperty = marshallProperty;
   }

   public byte[] getOrigDestBytes() {
      return origDestBytes;
   }

   public void setOrigDestBytes(byte[] origDestBytes) {
      this.origDestBytes = origDestBytes;
   }

   public byte[] getReplyToDestBytes() {
      return replyToDestBytes;
   }

   public void setReplyToDestBytes(byte[] replyToDestBytes) {
      this.replyToDestBytes = replyToDestBytes;
   }

   public int getGroupSequence() {
      return groupSequence;
   }

   public void setGroupSequence(int groupSequence) {
      this.groupSequence = groupSequence;
   }

   public boolean isDroppable() {
      return droppable;
   }

   public void setDroppable(boolean droppable) {
      this.droppable = droppable;
   }

   public byte[] getOrigTxId() {
      return origTxId;
   }

   public void setOrigTxId(byte[] origTxId) {
      this.origTxId = origTxId;
   }

   @Override
   public OpenwireMessage copy() {
      OpenwireMessage owMsg = new OpenwireMessage(this);
      owMsg.setArrival(this.arrival);
      owMsg.setBrokerInTime(this.brokerInTime);
      owMsg.setBroketPath(this.broketPath);
      owMsg.setCluster(this.cluster);
      owMsg.setCommandId(this.commandId);
      owMsg.setCompressed(this.compressed);
      owMsg.setDataStructure(this.dataStructure);
      owMsg.setDroppable(this.droppable);
      owMsg.setGroupSequence(this.groupSequence);
      owMsg.setMarshallProperty(this.marshallProperty);
      owMsg.setMessageId(this.messageId);
      owMsg.setOrigDestBytes(this.origDestBytes);
      owMsg.setOrigTxId(this.origTxId);
      owMsg.setProducerId(this.producerId);
      owMsg.setReplyToDestBytes(this.replyToDestBytes);
      return owMsg;
   }
}