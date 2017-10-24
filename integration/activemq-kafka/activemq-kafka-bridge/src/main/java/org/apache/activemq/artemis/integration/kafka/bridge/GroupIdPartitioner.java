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
package org.apache.activemq.artemis.integration.kafka.bridge;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

public class GroupIdPartitioner implements Partitioner {

   DefaultPartitioner defaultPartitioner = new DefaultPartitioner();

   @Override
   public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
      Message message = (Message) value;
      final SimpleString groupdID = message.getGroupID();
      //If GroupID (a.k.a JMSXGroupID) then to honour this we partition based on it.
      if (groupdID != null) {
         List partitions = cluster.partitionsForTopic(topic);
         int numPartitions = partitions.size();
         return Utils.toPositive(Utils.murmur2(groupdID.getData())) % numPartitions;
      } else {
         //Else we default back to defaultPartitioner and partition on key.
         return defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
      }
   }

   @Override
   public void close() {
      defaultPartitioner.close();
   }

   @Override
   public void configure(Map<String, ?> configs) {
      defaultPartitioner.configure(configs);
   }
}
