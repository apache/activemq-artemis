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

public class CheckFailoverReplyMessage extends PacketImpl {

   private boolean okToFailover;

   public CheckFailoverReplyMessage(boolean okToFailover) {
      super(CHECK_FOR_FAILOVER_REPLY);
      this.okToFailover = okToFailover;
   }

   public CheckFailoverReplyMessage() {
      super(CHECK_FOR_FAILOVER_REPLY);
   }

   @Override
   public boolean isResponse() {
      return true;
   }

   @Override
   public void encodeRest(ActiveMQBuffer buffer) {
      buffer.writeBoolean(okToFailover);
   }

   @Override
   public void decodeRest(ActiveMQBuffer buffer) {
      okToFailover = buffer.readBoolean();
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", okToFailover=" + okToFailover);
      buff.append("]");
      return buff.toString();
   }

   public boolean isOkToFailover() {
      return okToFailover;
   }
}
