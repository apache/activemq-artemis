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
package org.hornetq.jms.example;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;


/**
 * A Sender
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class Sender extends ClientAbstract
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MessageProducer producer;
   protected Queue queue;

   protected long msgs = TXRestartSoak.MIN_MESSAGES_ON_QUEUE;
   protected int pendingMsgs = 0;

   protected final Receiver[] receivers;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Sender(final Receiver[] receivers)
   {
      this.receivers = receivers;
   }

   @Override
   protected void connectClients() throws Exception
   {
      queue = (Queue)ctx.lookup("/queue/inputQueue");
      producer = sess.createProducer(queue);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
   }

   public void run()
   {
      super.run();
      while (running)
      {
         try
         {
            beginTX();
            for (int i = 0 ; i < 1000; i++)
            {
               BytesMessage msg = sess.createBytesMessage();
               msg.setLongProperty("count", pendingMsgs + msgs);
               msg.writeBytes(new byte[10 * 1024]);
               producer.send(msg);
               pendingMsgs++;
            }
            endTX();
         }
         catch (Exception e)
         {
            connect();
         }
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.example.ClientAbstract#onCommit()
    */
   @Override
   protected void onCommit()
   {
      this.msgs += pendingMsgs;
      for (Receiver rec : receivers)
      {
         rec.messageProduced(pendingMsgs);
      }

      pendingMsgs = 0;
      System.out.println("commit on sender msgs = " + msgs );
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.example.ClientAbstract#onRollback()
    */
   @Override
   protected void onRollback()
   {
      pendingMsgs = 0;
      System.out.println("Rolled back msgs=" + msgs);
   }

   public String toString()
   {
      return "Sender, msgs=" + msgs + ", pending=" + pendingMsgs;

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
