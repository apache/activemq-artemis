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
import javax.jms.MessageConsumer;
import javax.jms.Session;

/**
 *
 * A RecoveringReceiver.
 *
 * A Receiver that receives messages from a destination and alternately
 * acknowledges and recovers the session.
 * Must be used with ack mode CLIENT_ACKNOWLEDGE
 *
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RecoveringReceiver extends Receiver
{
   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   protected int ackSize;

   protected int recoverSize;

   class Count
   {
      int lastAcked;

      int lastReceived;
   }

   public RecoveringReceiver(final Session sess,
                             final MessageConsumer cons,
                             final int numMessages,
                             final int ackSize,
                             final int recoverSize,
                             final boolean isListener) throws Exception
   {
      super(sess, cons, numMessages, isListener);
      this.ackSize = ackSize;
      this.recoverSize = recoverSize;
   }

   @Override
   public void run()
   {
      // Small pause so as not to miss any messages in a topic
      try
      {
         Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {
      }

      try
      {
         int iterations = numMessages / ackSize;

         for (int outerCount = 0; outerCount < iterations; outerCount++)
         {
            Message m = null;
            for (int innerCount = 0; innerCount < ackSize; innerCount++)
            {
               m = getMessage();

               if (m == null)
               {
                  RecoveringReceiver.log.error("Message is null");
                  setFailed(true);
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));

               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     RecoveringReceiver.log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     setFailed(true);
                     return;
                  }
                  else
                  {
                     count = new Count();
                     counts.put(prodName, count);
                  }
               }
               else
               {
                  if (count.lastAcked != msgCount.intValue() - 1)
                  {
                     RecoveringReceiver.log.error("Message out of sequence for " + prodName +
                                                  ", expected " +
                                                  (count.lastAcked + 1));
                     setFailed(true);
                     return;
                  }
               }
               count.lastAcked = msgCount.intValue();

               count.lastReceived = msgCount.intValue();

               if (innerCount == ackSize - 1)
               {
                  m.acknowledge();
               }
               processingDone();

            }

            if (outerCount == iterations - 1)
            {
               break;
            }

            for (int innerCount = 0; innerCount < recoverSize; innerCount++)
            {
               m = getMessage();

               if (m == null)
               {
                  RecoveringReceiver.log.error("Message is null");
                  return;
               }
               String prodName = m.getStringProperty("PROD_NAME");
               Integer msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));

               Count count = (Count)counts.get(prodName);
               if (count == null)
               {
                  // First time
                  if (msgCount.intValue() != 0)
                  {
                     RecoveringReceiver.log.error("First message from " + prodName + " is not 0, it is " + msgCount);
                     setFailed(true);
                     return;
                  }
                  else
                  {
                     count = new Count();
                     count.lastAcked = -1;
                     counts.put(prodName, count);
                  }
               }
               else
               {
                  if (count.lastReceived != msgCount.intValue() - 1)
                  {
                     RecoveringReceiver.log.error("Message out of sequence");
                     setFailed(true);
                     return;
                  }
               }
               count.lastReceived = msgCount.intValue();

               if (innerCount == recoverSize - 1)
               {
                  sess.recover();
               }
               processingDone();
            }
         }
      }
      catch (Exception e)
      {
         RecoveringReceiver.log.error("Failed to receive message", e);
         setFailed(true);
      }
   }

}
