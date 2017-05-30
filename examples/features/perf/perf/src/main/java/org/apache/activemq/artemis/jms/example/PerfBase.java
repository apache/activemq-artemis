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
package org.apache.activemq.artemis.jms.example;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.activemq.artemis.utils.TokenBucketLimiter;
import org.apache.activemq.artemis.utils.TokenBucketLimiterImpl;
import org.apache.qpid.jms.JmsConnectionFactory;

public abstract class PerfBase {

   private static final Logger log = Logger.getLogger(PerfSender.class.getName());

   private static final String DEFAULT_PERF_PROPERTIES_FILE_NAME = "target/classes/perf.properties";

   private static byte[] randomByteArray(final int length) {
      byte[] bytes = new byte[length];

      Random random = new Random();

      for (int i = 0; i < length; i++) {
         bytes[i] = Integer.valueOf(random.nextInt()).byteValue();
      }

      return bytes;
   }

   protected static String getPerfFileName(final String[] args) {
      String fileName;

      if (args != null && args.length > 0) {
         fileName = args[0];
      } else {
         fileName = PerfBase.DEFAULT_PERF_PROPERTIES_FILE_NAME;
      }

      return fileName;
   }

   protected static PerfParams getParams(final String fileName) throws Exception {
      Properties props = null;

      try (InputStream is = new FileInputStream(fileName)) {
         props = new Properties();

         props.load(is);
      }

      int noOfMessages = Integer.valueOf(props.getProperty("num-messages"));
      int noOfWarmupMessages = Integer.valueOf(props.getProperty("num-warmup-messages"));
      int messageSize = Integer.valueOf(props.getProperty("message-size"));
      boolean durable = Boolean.valueOf(props.getProperty("durable"));
      boolean transacted = Boolean.valueOf(props.getProperty("transacted"));
      int batchSize = Integer.valueOf(props.getProperty("batch-size"));
      boolean drainQueue = Boolean.valueOf(props.getProperty("drain-queue"));
      String destinationName = props.getProperty("destination-name");
      int throttleRate = Integer.valueOf(props.getProperty("throttle-rate"));
      boolean dupsOK = Boolean.valueOf(props.getProperty("dups-ok-acknowledge"));
      boolean disableMessageID = Boolean.valueOf(props.getProperty("disable-message-id"));
      boolean disableTimestamp = Boolean.valueOf(props.getProperty("disable-message-timestamp"));
      String clientLibrary = props.getProperty("client-library", "core");
      String uri = props.getProperty("server-uri", "tcp://localhost:61616");

      PerfBase.log.info("num-messages: " + noOfMessages);
      PerfBase.log.info("num-warmup-messages: " + noOfWarmupMessages);
      PerfBase.log.info("message-size: " + messageSize);
      PerfBase.log.info("durable: " + durable);
      PerfBase.log.info("transacted: " + transacted);
      PerfBase.log.info("batch-size: " + batchSize);
      PerfBase.log.info("drain-queue: " + drainQueue);
      PerfBase.log.info("throttle-rate: " + throttleRate);
      PerfBase.log.info("destination-name: " + destinationName);
      PerfBase.log.info("disable-message-id: " + disableMessageID);
      PerfBase.log.info("disable-message-timestamp: " + disableTimestamp);
      PerfBase.log.info("dups-ok-acknowledge: " + dupsOK);
      PerfBase.log.info("server-uri: " + uri);
      PerfBase.log.info("client-library:" + clientLibrary);

      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setMessageSize(messageSize);
      perfParams.setDurable(durable);
      perfParams.setSessionTransacted(transacted);
      perfParams.setBatchSize(batchSize);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setDestinationName(destinationName);
      perfParams.setThrottleRate(throttleRate);
      perfParams.setDisableMessageID(disableMessageID);
      perfParams.setDisableTimestamp(disableTimestamp);
      perfParams.setDupsOK(dupsOK);
      perfParams.setLibraryType(clientLibrary);
      perfParams.setUri(uri);

      return perfParams;
   }

   private final PerfParams perfParams;

   protected PerfBase(final PerfParams perfParams) {
      this.perfParams = perfParams;
   }

   private ConnectionFactory factory;

   private Connection connection;

   private Session session;

   private Destination destination;

   private long start;

   private void init() throws Exception {
      if (perfParams.isOpenwire()) {
         factory = new org.apache.activemq.ActiveMQConnectionFactory(perfParams.getUri());

         destination = new org.apache.activemq.command.ActiveMQQueue(perfParams.getDestinationName());

         connection = factory.createConnection();
      } else if (perfParams.isCore()) {
         factory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(perfParams.getUri());

         destination = new org.apache.activemq.artemis.jms.client.ActiveMQQueue(perfParams.getDestinationName());

         connection = factory.createConnection();

      } else if (perfParams.isAMQP()) {
         factory = new JmsConnectionFactory(perfParams.getUri());

         destination = new org.apache.activemq.artemis.jms.client.ActiveMQQueue(perfParams.getDestinationName());

         connection = factory.createConnection();

         Session sessionX = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         destination = sessionX.createQueue(perfParams.getDestinationName());

         sessionX.close();
      }

      session = connection.createSession(perfParams.isSessionTransacted(), perfParams.isDupsOK() ? Session.DUPS_OK_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE);
   }

   private void displayAverage(final long numberOfMessages, final long start, final long end) {
      double duration = (1.0 * end - start) / 1000; // in seconds
      double average = 1.0 * numberOfMessages / duration;
      PerfBase.log.info(String.format("average: %.2f msg/s (%d messages in %2.2fs)", average, numberOfMessages, duration));
   }

   protected void runSender() {
      try {
         init();

         if (perfParams.isDrainQueue()) {
            drainQueue();
         }

         start = System.currentTimeMillis();
         PerfBase.log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(), perfParams.getBatchSize(), perfParams.isDurable(), perfParams.isSessionTransacted(), false, perfParams.getThrottleRate(), perfParams.getMessageSize());
         PerfBase.log.info("warmed up");
         start = System.currentTimeMillis();
         sendMessages(perfParams.getNoOfMessagesToSend(), perfParams.getBatchSize(), perfParams.isDurable(), perfParams.isSessionTransacted(), true, perfParams.getThrottleRate(), perfParams.getMessageSize());
         long end = System.currentTimeMillis();
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (session != null) {
            try {
               session.close();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
         if (connection != null) {
            try {
               connection.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

   protected void runListener() {
      try {
         init();

         if (perfParams.isDrainQueue()) {
            drainQueue();
         }

         MessageConsumer consumer = session.createConsumer(destination);

         connection.start();

         PerfBase.log.info("READY!!!");

         CountDownLatch countDownLatch = new CountDownLatch(1);
         consumer.setMessageListener(new PerfListener(countDownLatch, perfParams));
         countDownLatch.await();
         long end = System.currentTimeMillis();
         // start was set on the first received message
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (session != null) {
            try {
               session.close();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
         if (connection != null) {
            try {
               connection.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

   private void drainQueue() throws Exception {
      PerfBase.log.info("Draining queue");

      Session drainSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer consumer = drainSession.createConsumer(destination);

      connection.start();

      Message message = null;

      int count = 0;
      do {
         message = consumer.receive(3000);

         if (message != null) {
            message.acknowledge();

            count++;
         }
      }
      while (message != null);

      drainSession.close();

      PerfBase.log.info("Drained " + count + " messages");
   }

   private void sendMessages(final int numberOfMessages,
                             final int txBatchSize,
                             final boolean durable,
                             final boolean transacted,
                             final boolean display,
                             final int throttleRate,
                             final int messageSize) throws Exception {
      MessageProducer producer = session.createProducer(destination);

      producer.setDeliveryMode(perfParams.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

      producer.setDisableMessageID(perfParams.isDisableMessageID());

      producer.setDisableMessageTimestamp(perfParams.isDisableTimestamp());

      BytesMessage message = session.createBytesMessage();

      byte[] payload = PerfBase.randomByteArray(messageSize);

      message.writeBytes(payload);

      final int modulo = 2000;

      TokenBucketLimiter tbl = throttleRate != -1 ? new TokenBucketLimiterImpl(throttleRate, false) : null;

      boolean committed = false;
      for (int i = 1; i <= numberOfMessages; i++) {
         producer.send(message);

         if (transacted) {
            if (i % txBatchSize == 0) {
               session.commit();
               committed = true;
            } else {
               committed = false;
            }
         }

         if (display && i % modulo == 0) {
            double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
            PerfBase.log.info(String.format("sent %6d messages in %2.2fs", i, duration));
         }

         if (tbl != null) {
            tbl.limit();
         }
      }
      if (transacted && !committed) {
         session.commit();
      }
   }

   private class PerfListener implements MessageListener {

      private final CountDownLatch countDownLatch;

      private final PerfParams perfParams;

      private boolean warmingUp = true;

      private boolean started = false;

      private final int modulo;

      private final AtomicLong count = new AtomicLong(0);

      private PerfListener(final CountDownLatch countDownLatch, final PerfParams perfParams) {
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
         warmingUp = perfParams.getNoOfWarmupMessages() > 0;
         modulo = 2000;
      }

      @Override
      public void onMessage(final Message message) {
         try {
            if (warmingUp) {
               boolean committed = checkCommit();
               if (count.incrementAndGet() == perfParams.getNoOfWarmupMessages()) {
                  PerfBase.log.info("warmed up after receiving " + count.longValue() + " msgs");
                  if (!committed) {
                     checkCommit();
                  }
                  warmingUp = false;
               }
               return;
            }

            if (!started) {
               started = true;
               // reset count to take stats
               count.set(0);
               start = System.currentTimeMillis();
            }

            long currentCount = count.incrementAndGet();
            boolean committed = checkCommit();
            if (currentCount == perfParams.getNoOfMessagesToSend()) {
               if (!committed) {
                  checkCommit();
               }
               countDownLatch.countDown();
            }
            if (currentCount % modulo == 0) {
               double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
               PerfBase.log.info(String.format("received %6d messages in %2.2fs", currentCount, duration));
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      private boolean checkCommit() throws Exception {
         if (perfParams.isSessionTransacted()) {
            if (count.longValue() % perfParams.getBatchSize() == 0) {
               session.commit();

               return true;
            }
         }
         return false;
      }
   }

}
