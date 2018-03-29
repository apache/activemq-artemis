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

package org.apache.activemq.artemis.cli.commands.messages;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.cli.commands.ActionContext;

@Command(name = "consumer", description = "It will consume messages from an instance")
public class Consumer extends DestAbstract {

   @Option(name = "--durable", description = "It will use durable subscription in case of client")
   boolean durable = false;

   @Option(name = "--break-on-null", description = "It will break on null messages")
   boolean breakOnNull = false;

   @Option(name = "--receive-timeout", description = "Time used on receive(timeout)")
   int receiveTimeout = 3000;

   @Option(name = "--filter", description = "filter to be used with the consumer")
   String filter;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      System.out.println("Consumer:: filter = " + filter);

      ConnectionFactory factory = createConnectionFactory();

      try (Connection connection = factory.createConnection()) {
         ConsumerThread[] threadsArray = new ConsumerThread[threads];
         for (int i = 0; i < threads; i++) {
            Session session;
            if (txBatchSize > 0) {
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }
            Destination dest = lookupDestination(session);
            threadsArray[i] = new ConsumerThread(session, dest, i);

            threadsArray[i].setVerbose(verbose).setSleep(sleep).setDurable(durable).setBatchSize(txBatchSize).setBreakOnNull(breakOnNull).setMessageCount(messageCount).setReceiveTimeOut(receiveTimeout).setFilter(filter).setBrowse(false);
         }

         for (ConsumerThread thread : threadsArray) {
            thread.start();
         }

         connection.start();

         int received = 0;

         for (ConsumerThread thread : threadsArray) {
            thread.join();
            received += thread.getReceived();
         }

         return received;
      }
   }


}
