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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.hornetq.common.example.HornetQExample;

/**
 *
 * This is used as a soak test to verify HornetQ's capability of persistent messages over restarts.
 *
 * This is used as a smoke test before releases.
 *
 * WARNING: This is not a sample on how you should handle XA.
 *          You are supposed to use a TransactionManager.
 *          This class is doing the job of a TransactionManager that fits for the purpose of this test only,
 *          however there are many more pitfalls to deal with Transactions.
 *
 *          This is just to stress and soak test Transactions with HornetQ.
 *
 *          And this is dealing with XA directly for the purpose testing only.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class TXRestartSoak extends HornetQExample
{

   public static final int MIN_MESSAGES_ON_QUEUE = 50000;

   private static final Logger log = Logger.getLogger(TXRestartSoak.class.getName());

   public static void main(final String[] args)
   {
      new TXRestartSoak().run(args);
   }

   private TXRestartSoak()
   {
      super();
   }

   /* (non-Javadoc)
    * @see org.hornetq.common.example.HornetQExample#runExample()
    */
   @Override
   public boolean runExample() throws Exception
   {

      Connection connection = null;
      InitialContext initialContext = null;

      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create the JMS objects
         connection = cf.createConnection();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/inputQueue");

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer = session.createProducer(queue);

         for (int i = 0 ; i < MIN_MESSAGES_ON_QUEUE; i++)
         {
            BytesMessage msg = session.createBytesMessage();
            msg.setLongProperty("count", i);
            msg.writeBytes(new byte[10 * 1024]);
            producer.send(msg);

            if (i % 1000 == 0)
            {
               System.out.println("Sent " + i + " messages");
               session.commit();
            }
         }

         session.commit();

         Receiver rec1 = new Receiver("/queue/diverted1");
         Receiver rec2 = new Receiver("/queue/diverted2");

         Sender send = new Sender(new Receiver[]{rec1, rec2});

         send.start();
         rec1.start();
         rec2.start();

         long timeEnd = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);

         if (runServer)
         {
            while (timeEnd > System.currentTimeMillis())
            {
               System.out.println("Letting the service run for 20 seconds");
               Thread.sleep(TimeUnit.SECONDS.toMillis(20));
               stopServers();

               Thread.sleep(10000);

               boolean disconnected = false;

               if (send.getErrorsCount() != 0 || rec1.getErrorsCount() != 0 || rec2.getErrorsCount() != 0)
               {
                  System.out.println("There are sequence errors in some of the clients, please look at the logs");
                  break;
               }

               while (!disconnected)
               {
                  disconnected = send.getConnection() == null && rec1.getConnection() == null && rec2.getConnection() == null;
                  if (!disconnected)
                  {
                     System.out.println("NOT ALL THE CLIENTS ARE DISCONNECTED, NEED TO WAIT THEM");
                  }
                  Thread.sleep(1000);
               }

               startServers();
            }
         }
         else
         {
            while (timeEnd > System.currentTimeMillis())
            {
               if (send.getErrorsCount() != 0 || rec1.getErrorsCount() != 0 || rec2.getErrorsCount() != 0)
               {
                  System.out.println("There are sequence errors in some of the clients, please look at the logs");
                  break;
               }
               Thread.sleep(10000);
            }
         }

         send.setRunning(false);
         rec1.setRunning(false);
         rec2.setRunning(false);

         send.join();
         rec1.join();
         rec2.join();

         return send.getErrorsCount() == 0 && rec1.getErrorsCount() == 0 && rec2.getErrorsCount() == 0;

      }
      finally
      {
         connection.close();
      }

   }
}
