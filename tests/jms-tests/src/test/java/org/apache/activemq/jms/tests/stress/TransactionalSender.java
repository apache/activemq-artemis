/**
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
package org.apache.activemq.jms.tests.stress;

import org.apache.activemq.jms.tests.JmsTestLogger;

import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 *
 * A Sender that sends messages to a destination in a JMS transaction.
 *
 * Sends messages to a destination in a jms transaction.
 * Sends <commitSize> messages then commits, then
 * sends <rollbackSize> messages then rollsback until
 * a total of <numMessages> messages have been sent (commitSize)
 * <numMessages> must be a multiple of <commitSize>
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransactionalSender extends Sender
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected int commitSize;

   protected int rollbackSize;

   public TransactionalSender(final String prodName,
                              final Session sess,
                              final MessageProducer prod,
                              final int numMessages,
                              final int commitSize,
                              final int rollbackSize)
   {
      super(prodName, sess, prod, numMessages);

      this.commitSize = commitSize;
      this.rollbackSize = rollbackSize;

   }

   @Override
   public void run()
   {
      int iterations = numMessages / commitSize;

      try
      {
         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            for (int innerCount = 0; innerCount < commitSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", outerCount * commitSize + innerCount);
               prod.send(m);
            }
            sess.commit();
            for (int innerCount = 0; innerCount < rollbackSize; innerCount++)
            {
               Message m = sess.createMessage();
               m.setStringProperty("PROD_NAME", prodName);
               m.setIntProperty("MSG_NUMBER", (outerCount + 1) * commitSize + innerCount);
               prod.send(m);
            }
            sess.rollback();
         }
      }
      catch (Exception e)
      {
         TransactionalSender.log.error("Failed to send message", e);
         setFailed(true);
      }
   }
}
