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
package org.apache.activemq.artemis.jms.client;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;

public final class JMSExceptionHelper {

   public static JMSException convertFromActiveMQException(final ActiveMQInterruptedException me) {
      JMSException je = new javax.jms.IllegalStateException(me.getMessage());

      je.setStackTrace(me.getStackTrace());

      je.initCause(me);
      return je;
   }

   public static JMSException convertFromActiveMQException(final ActiveMQException me) {
      JMSException je;
      switch (me.getType()) {
         case CONNECTION_TIMEDOUT:
         case INTERNAL_ERROR:
         case NOT_CONNECTED:
            je = new JMSException(me.getMessage());
            break;

         case UNSUPPORTED_PACKET:
         case OBJECT_CLOSED:
         case ILLEGAL_STATE:
            je = new javax.jms.IllegalStateException(me.getMessage());
            break;

         case QUEUE_DOES_NOT_EXIST:
         case QUEUE_EXISTS:
            je = new InvalidDestinationException(me.getMessage());
            break;

         case INVALID_FILTER_EXPRESSION:
            je = new InvalidSelectorException(me.getMessage());
            break;

         case SECURITY_EXCEPTION:
            je = new JMSSecurityException(me.getMessage());
            break;

         case TRANSACTION_ROLLED_BACK:
            je = new javax.jms.TransactionRolledBackException(me.getMessage());
            break;

         default:
            je = new JMSException(me.getMessage());
      }

      je.setStackTrace(me.getStackTrace());

      je.initCause(me);

      return je;
   }
}
