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

public enum CommandType {

   CONNECT(Stomp.Commands.CONNECT),
   SEND(Stomp.Commands.SEND),
   DISCONNECT(Stomp.Commands.DISCONNECT),
   SUBSCRIBE(Stomp.Commands.SUBSCRIBE),
   UNSUBSCRIBE(Stomp.Commands.UNSUBSCRIBE),
   BEGIN(Stomp.Commands.BEGIN),
   COMMIT(Stomp.Commands.COMMIT),
   ABORT(Stomp.Commands.ABORT),
   ACK(Stomp.Commands.ACK),
   NACK(Stomp.Commands.NACK),
   STOMP(Stomp.Commands.STOMP),
   CONNECTED(Stomp.Responses.CONNECTED),
   ERROR(Stomp.Responses.ERROR),
   MESSAGE(Stomp.Responses.MESSAGE),
   RECEIPT(Stomp.Responses.RECEIPT);

   private String type;

   CommandType(String command) {
      type = command;
   }

   @Override
   public String toString() {
      return type;
   }

   public static CommandType getType(String command) {
      switch (command) {
         case Stomp.Commands.CONNECT:
            return CONNECT;
         case Stomp.Commands.SEND:
            return SEND;
         case Stomp.Commands.DISCONNECT:
            return DISCONNECT;
         case Stomp.Commands.SUBSCRIBE:
            return SUBSCRIBE;
         case Stomp.Commands.UNSUBSCRIBE:
            return UNSUBSCRIBE;
         case Stomp.Commands.BEGIN:
            return BEGIN;
         case Stomp.Commands.COMMIT:
            return COMMIT;
         case Stomp.Commands.ABORT:
            return ABORT;
         case Stomp.Commands.ACK:
            return ACK;
         case Stomp.Commands.NACK:
            return NACK;
         case Stomp.Commands.STOMP:
            return STOMP;
         case Stomp.Responses.CONNECTED:
            return CONNECTED;
         case Stomp.Responses.ERROR:
            return ERROR;
         case Stomp.Responses.MESSAGE:
            return MESSAGE;
         case Stomp.Responses.RECEIPT:
            return RECEIPT;
         default:
            throw new IllegalArgumentException("Unrecognized command: " + command);
      }
   }
}
