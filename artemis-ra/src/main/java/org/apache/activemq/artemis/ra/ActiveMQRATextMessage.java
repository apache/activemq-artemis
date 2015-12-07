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
import javax.jms.TextMessage;

/**
 * A wrapper for a message
 */
public class ActiveMQRATextMessage extends ActiveMQRAMessage implements TextMessage {

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
   public ActiveMQRATextMessage(final TextMessage message, final ActiveMQRASession session) {
      super(message, session);

      if (ActiveMQRATextMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get text
    *
    * @return The text
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public String getText() throws JMSException {
      if (ActiveMQRATextMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("getText()");
      }

      return ((TextMessage) message).getText();
   }

   /**
    * Set text
    *
    * @param string The text
    * @throws JMSException Thrown if an error occurs
    */
   @Override
   public void setText(final String string) throws JMSException {
      if (ActiveMQRATextMessage.trace) {
         ActiveMQRALogger.LOGGER.trace("setText(" + string + ")");
      }

      ((TextMessage) message).setText(string);
   }
}
