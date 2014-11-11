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
package org.apache.activemq6.jms.tests.stress;

import org.apache.activemq6.jms.tests.JmsTestLogger;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 *
 * A Sender.
 *
 * Sends messages to a destination, used in stress testing
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Sender extends Runner
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected MessageProducer prod;

   protected String prodName;

   protected int count;

   public Sender(final String prodName, final Session sess, final MessageProducer prod, final int numMessages)
   {
      super(sess, numMessages);
      this.prod = prod;
      this.prodName = prodName;
   }

   @Override
   public void run()
   {
      try
      {
         while (count < numMessages)
         {
            Message m = sess.createMessage();
            m.setStringProperty("PROD_NAME", prodName);
            m.setIntProperty("MSG_NUMBER", count);
            prod.send(m);
            count++;
         }
      }
      catch (Exception e)
      {
         Sender.log.error("Failed to send message", e);
         setFailed(true);
      }
   }

}
