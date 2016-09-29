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
package org.apache.activemq.artemis.jms.soak.example;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.activemq.artemis.utils.TokenBucketLimiter;
import org.apache.activemq.artemis.utils.TokenBucketLimiterImpl;

public class SoakSender {

   private static final Logger log = Logger.getLogger(SoakSender.class.getName());

   public static void main(final String[] args) {
      try {
         String fileName = SoakBase.getPerfFileName();

         SoakParams params = SoakBase.getParams(fileName);
         final SoakSender sender = new SoakSender(params);

         Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
               sender.disconnect();
            }
         });

         sender.run();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private final SoakParams perfParams;

   private Connection connection;

   private Session session;

   private MessageProducer producer;

   private final ExceptionListener exceptionListener = new ExceptionListener() {
      @Override
      public void onException(final JMSException e) {
         System.out.println("SoakReconnectableSender.exceptionListener.new ExceptionListener() {...}.onException()");
         disconnect();
         connect();
      }

   };

   private SoakSender(final SoakParams perfParams) {
      this.perfParams = perfParams;
   }

   public void run() throws Exception {
      connect();

      boolean runInfinitely = perfParams.getDurationInMinutes() == -1;

      BytesMessage message = session.createBytesMessage();

      byte[] payload = SoakBase.randomByteArray(perfParams.getMessageSize());

      message.writeBytes(payload);

      final int modulo = 10000;

      TokenBucketLimiter tbl = perfParams.getThrottleRate() != -1 ? new TokenBucketLimiterImpl(perfParams.getThrottleRate(), false) : null;

      boolean transacted = perfParams.isSessionTransacted();
      int txBatchSize = perfParams.getBatchSize();
      boolean display = true;

      long start = System.currentTimeMillis();
      long moduleStart = start;
      AtomicLong count = new AtomicLong(0);
      while (true) {
         try {
            producer.send(message);
            count.incrementAndGet();

            if (transacted) {
               if (count.longValue() % txBatchSize == 0) {
                  session.commit();
               }
            }

            long totalDuration = System.currentTimeMillis() - start;

            if (display && count.longValue() % modulo == 0) {
               double duration = (1.0 * System.currentTimeMillis() - moduleStart) / 1000;
               moduleStart = System.currentTimeMillis();
               SoakSender.log.info(String.format("sent %s messages in %2.2fs (time: %.0fs)", modulo, duration, totalDuration / 1000.0));
            }

            if (tbl != null) {
               tbl.limit();
            }

            if (!runInfinitely && totalDuration > perfParams.getDurationInMinutes() * SoakBase.TO_MILLIS) {
               break;
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      SoakSender.log.info(String.format("Sent %s messages in %s minutes", count, perfParams.getDurationInMinutes()));
      SoakSender.log.info("END OF RUN");

      if (connection != null) {
         connection.close();
         connection = null;
      }
   }

   private synchronized void disconnect() {
      if (connection != null) {
         try {
            connection.setExceptionListener(null);
            connection.close();
         } catch (JMSException e) {
            e.printStackTrace();
         } finally {
            connection = null;
         }
      }
   }

   private void connect() {
      InitialContext ic = null;
      try {
         ic = new InitialContext();

         ConnectionFactory factory = (ConnectionFactory) ic.lookup(perfParams.getConnectionFactoryLookup());

         Destination destination = (Destination) ic.lookup(perfParams.getDestinationLookup());

         connection = factory.createConnection();

         session = connection.createSession(perfParams.isSessionTransacted(), perfParams.isDupsOK() ? Session.DUPS_OK_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE);

         producer = session.createProducer(destination);

         producer.setDeliveryMode(perfParams.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         producer.setDisableMessageID(perfParams.isDisableMessageID());

         producer.setDisableMessageTimestamp(perfParams.isDisableTimestamp());

         connection.setExceptionListener(exceptionListener);
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         try {
            ic.close();
         } catch (NamingException e) {
            e.printStackTrace();
         }
      }
   }
}
