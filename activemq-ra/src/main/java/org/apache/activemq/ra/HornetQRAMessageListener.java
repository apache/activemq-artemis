/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.ra;

import javax.jms.Message;
import javax.jms.MessageListener;


/**
 * A wrapper for a message listener
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAMessageListener implements MessageListener
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /** The message listener */
   private final MessageListener listener;

   /** The consumer */
   private final HornetQRAMessageConsumer consumer;

   /**
    * Create a new wrapper
    * @param listener the listener
    * @param consumer the consumer
    */
   public HornetQRAMessageListener(final MessageListener listener, final HornetQRAMessageConsumer consumer)
   {
      if (HornetQRAMessageListener.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + listener + ", " + consumer + ")");
      }

      this.listener = listener;
      this.consumer = consumer;
   }

   /**
    * On message
    * @param message The message
    */
   public void onMessage(Message message)
   {
      if (HornetQRAMessageListener.trace)
      {
         HornetQRALogger.LOGGER.trace("onMessage(" + message + ")");
      }

      message = consumer.wrapMessage(message);
      listener.onMessage(message);
   }
}
