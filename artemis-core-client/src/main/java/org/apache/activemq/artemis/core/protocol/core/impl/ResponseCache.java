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
package org.apache.activemq.artemis.core.protocol.core.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.ResponseHandler;
import org.apache.activemq.artemis.utils.collections.ConcurrentLongHashMap;

public class ResponseCache {

   private final AtomicLong sequence = new AtomicLong(0);

   private final ConcurrentLongHashMap<Packet> store;
   private final int size;
   private ResponseHandler responseHandler;

   public ResponseCache(int size) {
      this.store = new ConcurrentLongHashMap<>(size);
      this.size = size;
   }

   public long nextCorrelationID() {
      return sequence.incrementAndGet();
   }

   public void add(Packet packet) {
      if (store.size() + 1 > size) {
         throw ActiveMQClientMessageBundle.BUNDLE.cannotSendPacketWhilstResponseCacheFull();
      }
      this.store.put(packet.getCorrelationID(), packet);
   }

   public void handleResponse(Packet response) {
      long correlationID = response.getCorrelationID();
      Packet packet = store.remove(correlationID);
      if (packet != null) {
         responseHandler.responseHandler(packet, response);
      }
   }

   public void setResponseHandler(ResponseHandler responseHandler) {
      this.responseHandler = responseHandler;
   }
}
