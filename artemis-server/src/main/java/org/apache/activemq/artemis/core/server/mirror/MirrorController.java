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
package org.apache.activemq.artemis.core.server.mirror;

import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;


/**
 * This represents the contract we will use to send messages to replicas.
 * */
public interface MirrorController {
   void startAddressScan() throws Exception;
   void endAddressScan() throws Exception;

   void addAddress(AddressInfo addressInfo) throws Exception;
   void deleteAddress(AddressInfo addressInfo) throws Exception;
   void createQueue(QueueConfiguration queueConfiguration) throws Exception;
   void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception;
   void sendMessage(Message message, RoutingContext context, List<MessageReference> refs);

   void postAcknowledge(MessageReference ref, AckReason reason) throws Exception;
}
