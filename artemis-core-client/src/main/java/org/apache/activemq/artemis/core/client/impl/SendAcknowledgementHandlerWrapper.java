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

package org.apache.activemq.artemis.core.client.impl;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;

public class SendAcknowledgementHandlerWrapper implements SendAcknowledgementHandler {

   private SendAcknowledgementHandler wrapped;

   /**
    * It's possible that a SendAcknowledgementHandler might be called twice due to subsequent
    * packet confirmations on the same connection. Using this boolean avoids that possibility.
    * A new SendAcknowledgementHandlerWrapper is created for each message sent so once it's
    * called once it will never be called again.
    */
   private volatile boolean active = true;

   public SendAcknowledgementHandlerWrapper(SendAcknowledgementHandler wrapped) {
      this.wrapped = wrapped;
   }

   @Override
   public void sendAcknowledged(Message message) {
      if (active) {
         try {
            wrapped.sendAcknowledged(message);
         } finally {
            active = false;
         }
      }
   }

   @Override
   public void sendFailed(Message message, Exception e) {
      if (active) {
         try {
            wrapped.sendFailed(message, e);
         } finally {
            active = false;
         }
      }
   }
}
