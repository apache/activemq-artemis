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
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public class ActiveMQExceptionMessage extends PacketImpl {

   private ActiveMQException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ActiveMQExceptionMessage(final ActiveMQException exception) {
      super(EXCEPTION);

      this.exception = exception;
   }

   public ActiveMQExceptionMessage() {
      super(EXCEPTION);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse() {
      return true;
   }

   public ActiveMQException getException() {
      return exception;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeInt(exception.getType().getCode());
      buffer.writeNullableString(exception.getMessage());
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      int code = buffer.readInt();
      String msg = buffer.readNullableString();

      exception = ActiveMQExceptionType.createException(code, msg);
   }

   @Override
   public String toString() {
      return getParentString() + ", exception= " + exception + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((exception == null) ? 0 : exception.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof ActiveMQExceptionMessage)) {
         return false;
      }
      ActiveMQExceptionMessage other = (ActiveMQExceptionMessage) obj;
      if (exception == null) {
         if (other.exception != null) {
            return false;
         }
      } else if (!exception.equals(other.exception)) {
         return false;
      }
      return true;
   }
}
