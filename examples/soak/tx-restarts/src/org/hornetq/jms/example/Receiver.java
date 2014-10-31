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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;

import org.hornetq.utils.ReusableLatch;

/**
 * A Receiver
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class Receiver extends ClientAbstract
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // We should leave some messages on paging. We don't want to consume all for this test
   private final Semaphore minConsume = new Semaphore(0);

   private final ReusableLatch latchMax = new ReusableLatch(0);

   private static final int MAX_DIFF = 10000;

   // The difference between producer and consuming
   private final AtomicInteger currentDiff = new AtomicInteger(0);

   private final String queueJNDI;

   protected long msgs = 0;

   protected int pendingMsgs = 0;

   protected int pendingSemaphores = 0;

   protected MessageConsumer cons;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Receiver(String queueJNDI)
   {
      super();
      this.queueJNDI = queueJNDI;
   }

   // Public --------------------------------------------------------

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
               Message msg = cons.receive(5000);
               if (msg == null)
               {
                  break;
               }

               if (msg.getLongProperty("count") != msgs + pendingMsgs)
               {
                  errors++;
                  System.out.println("count should be " + (msgs + pendingMsgs) + " when it was " + msg.getLongProperty("count") + " on " + queueJNDI);
               }

               pendingMsgs++;
               if (!minConsume.tryAcquire(1, 5, TimeUnit.SECONDS))
               {
                  break;
               }

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
    * @see org.hornetq.jms.example.ClientAbstract#connectClients()
    */
   @Override
   protected void connectClients() throws Exception
   {

      queue = (Queue)ctx.lookup(queueJNDI);

      cons = sess.createConsumer(queue);

      conn.start();
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.example.ClientAbstract#onCommit()
    */
   @Override
   protected void onCommit()
   {
      msgs += pendingMsgs;
      this.currentDiff.addAndGet(-pendingMsgs);
      latchMax.countDown(pendingMsgs);
      pendingMsgs = 0;
      System.out.println("Commit on consumer " + queueJNDI + ", msgs=" + msgs + " currentDiff = " + currentDiff);
   }

   /* (non-Javadoc)
    * @see org.hornetq.jms.example.ClientAbstract#onRollback()
    */
   @Override
   protected void onRollback()
   {
      System.out.println("Rollback on consumer " + queueJNDI + ", msgs=" + msgs);
      minConsume.release(pendingMsgs);
      pendingMsgs = 0;
   }

   public String toString()
   {
      return "Receiver::" + this.queueJNDI + ", msgs=" + msgs + ", pending=" + pendingMsgs;
   }

   /**
    * @param pendingMsgs2
    */
   public void messageProduced(int producedMessages)
   {
      minConsume.release(producedMessages);
      currentDiff.addAndGet(producedMessages);
      System.out.println("Msg produced on " + this.queueJNDI + ", currentDiff = " + currentDiff);
      if (currentDiff.get() > MAX_DIFF)
      {
         System.out.println("Holding producer for 5 seconds");
         latchMax.setCount(currentDiff.get() - MAX_DIFF);
         try
         {
            latchMax.await(5, TimeUnit.SECONDS);
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
