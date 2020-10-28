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
package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;

/** this will be used when there are multiple replicas in use. */
public class AMQPMirrorControllerAggregation implements MirrorController, ActiveMQComponent {


   List<AMQPMirrorControllerSource> partitions = new ArrayList<>();

   public void addPartition(AMQPMirrorControllerSource partition) {
      this.partitions.add(partition);
   }

   public void removeParition(AMQPMirrorControllerSource partition) {
      this.partitions.remove(partition);
   }


   @Override
   public void start() throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      for (MirrorController partition : partitions) {
         partition.addAddress(addressInfo);
      }

   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      for (MirrorController partition : partitions) {
         partition.deleteAddress(addressInfo);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      for (MirrorController partition : partitions) {
         partition.createQueue(queueConfiguration);
      }
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      for (MirrorController partition : partitions) {
         partition.deleteQueue(addressName, queueName);
      }
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
      for (MirrorController partition : partitions) {
         partition.sendMessage(message, context, refs);
      }
   }

   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) throws Exception {
      for (MirrorController partition : partitions) {
         partition.postAcknowledge(ref, reason);
      }
   }

   @Override
   public void startAddressScan() throws Exception {
      for (MirrorController partition : partitions) {
         partition.startAddressScan();
      }
   }

   @Override
   public void endAddressScan() throws Exception {
      for (MirrorController partition : partitions) {
         partition.endAddressScan();
      }
   }
}
