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
package org.apache.activemq.artemis.ra;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

/**
 * A wrapper for a message
 */
public class ActiveMQRAObjectMessage extends ActiveMQRAMessage implements ObjectMessage {

   /**
    * Whether trace is enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * Create a new wrapper
    *
    * @param message the message
    * @param session the session
    */
   public ActiveMQRAObjectMessage(final ObjectMessage message, final ActiveMQRASession session) {
      super(message, session);

      if (ActiveMQRAObjectMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get the object
    *
    * @return The object
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public Serializable getObject() throws JMSException {
      if (ActiveMQRAObjectMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("getObject()");
      }

      return ((ObjectMessage) message).getObject();
   }

   /**
    * Set the object
    *
    * @param object The object
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setObject(final Serializable object) throws JMSException {
      if (ActiveMQRAObjectMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("setObject(" + object + ")");
      }

      ((ObjectMessage) message).setObject(object);
   }
}
