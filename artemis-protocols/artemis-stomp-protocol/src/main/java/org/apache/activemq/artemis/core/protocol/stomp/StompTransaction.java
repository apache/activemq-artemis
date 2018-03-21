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
package org.apache.activemq.artemis.core.protocol.stomp;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ServerSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Control's stomp transaction processing
 * it goes with stomp connections.
 * it stores acks, sends, nacks
 * during commit it applies those to core sessions.
 * because each stomp connection uses only one session
 * we can't rely on core session to manage multiple
 * tx's.
 */
public class StompTransaction {

   private List<StompAck> acks = new ArrayList<>();
   private List<Message> sends = new ArrayList<>();

   public void commit(StompSession session) throws Exception {
      ServerSession serverSession = session.getCoreSession();
      for (Message m : sends) {
         serverSession.send(m, false);
      }
      for (StompAck ack : acks) {
         //same process for ack and nack
         if (ack.isIndividualAck) {
            serverSession.individualAcknowledge(ack.consumerID, ack.id);
         } else {
            serverSession.acknowledge(ack.consumerID, ack.id);
         }
      }
      serverSession.commit();
   }

   public void abort(StompSession session) throws Exception {
      acks.clear();
      sends.clear();
      session.getCoreSession().rollback(false);
   }

   public void addAck(long id, long consumerID, boolean isAck, boolean isIndividualAck) {
      acks.add(new StompAck(id, consumerID, isAck, isIndividualAck));
   }

   public void addSend(Message message) {
      sends.add(message);
   }

   private static class StompAck {
      public final long id;
      public final long consumerID;
      public final boolean isAck;
      public final boolean isIndividualAck;

      StompAck(long id, long consumerID, boolean isAck, boolean isIndividualAck) {
         this.id = id;
         this.consumerID = consumerID;
         this.isAck = isAck;
         this.isIndividualAck = isIndividualAck;
      }
   }
}
