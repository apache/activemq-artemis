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
package org.apache.activemq.artemis.cli.commands.util;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.utils.ReusableLatch;

public class ProducerThread extends Thread {

   protected final Session session;

   boolean verbose;
   int messageCount = 1000;
   boolean runIndefinitely = false;
   Destination destination;
   int sleep = 0;
   boolean persistent = true;
   int messageSize = 0;
   int textMessageSize;
   long msgTTL = 0L;
   String msgGroupID = null;
   int transactionBatchSize;

   int transactions = 0;
   final AtomicInteger sentCount = new AtomicInteger(0);
   String message;
   String messageText = null;
   String payloadUrl = null;
   byte[] payload = null;
   boolean running = false;
   final ReusableLatch finished = new ReusableLatch(1);
   final ReusableLatch paused = new ReusableLatch(0);

   public ProducerThread(Session session, Destination destination, int threadNr) {
      super("Producer " + destination.toString() + ", thread=" + threadNr);
      this.destination = destination;
      this.session = session;
   }

   @Override
   public void run() {
      MessageProducer producer = null;
      String threadName = Thread.currentThread().getName();
      try {
         producer = session.createProducer(destination);
         producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
         producer.setTimeToLive(msgTTL);
         initPayLoad();
         running = true;

         System.out.println(threadName + " Started to calculate elapsed time ...\n");
         long tStart = System.currentTimeMillis();

         if (runIndefinitely) {
            while (running) {
               paused.await();
               sendMessage(producer, threadName);
               sentCount.incrementAndGet();
            }
         } else {
            for (sentCount.set(0); sentCount.get() < messageCount && running; sentCount.incrementAndGet()) {
               paused.await();
               sendMessage(producer, threadName);
            }
         }

         try {
            session.commit();
         } catch (Throwable ignored) {
         }

         System.out.println(threadName + " Produced: " + this.getSentCount() + " messages");
         long tEnd = System.currentTimeMillis();
         long elapsed = (tEnd - tStart) / 1000;
         System.out.println(threadName + " Elapsed time in second : " + elapsed + " s");
         System.out.println(threadName + " Elapsed time in milli second : " + (tEnd - tStart) + " milli seconds");

      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         if (finished != null) {
            finished.countDown();
         }
         if (producer != null) {
            try {
               producer.close();
            } catch (JMSException e) {
               e.printStackTrace();
            }
         }
      }
   }

   private void sendMessage(MessageProducer producer, String threadName) throws Exception {
      Message message = createMessage(sentCount.get(), threadName);
      producer.send(message);
      if (verbose) {
         System.out.println(threadName + " Sent: " + (message instanceof TextMessage ? ((TextMessage) message).getText() : message.getJMSMessageID()));
      }

      if (transactionBatchSize > 0 && sentCount.get() > 0 && sentCount.get() % transactionBatchSize == 0) {
         System.out.println(threadName + " Committing transaction: " + transactions++);
         session.commit();
      }

      if (sleep > 0) {
         Thread.sleep(sleep);
      }
   }

   private void initPayLoad() {
      if (messageSize > 0) {
         payload = new byte[messageSize];
         for (int i = 0; i < payload.length; i++) {
            payload[i] = '.';
         }
      }
   }

   protected Message createMessage(int i, String threadName) throws Exception {
      Message answer;
      if (payload != null) {
         answer = session.createBytesMessage();
         ((BytesMessage) answer).writeBytes(payload);
      } else {
         if (textMessageSize > 0) {
            if (messageText == null) {
               messageText = readInputStream(getClass().getResourceAsStream("demo.txt"), textMessageSize, i);
            }
         } else if (payloadUrl != null) {
            messageText = readInputStream(new URL(payloadUrl).openStream(), -1, i);
         } else if (message != null) {
            messageText = message;
         } else {
            messageText = createDefaultMessage(i);
         }
         answer = session.createTextMessage(messageText);
      }
      if ((msgGroupID != null) && (!msgGroupID.isEmpty())) {
         answer.setStringProperty("JMSXGroupID", msgGroupID);
      }

      answer.setIntProperty("count", i);
      answer.setStringProperty("ThreadSent", threadName);
      return answer;
   }

   private String readInputStream(InputStream is, int size, int messageNumber) throws IOException {
      try (InputStreamReader reader = new InputStreamReader(is)) {
         char[] buffer;
         if (size > 0) {
            buffer = new char[size];
         } else {
            buffer = new char[1024];
         }
         int count;
         StringBuilder builder = new StringBuilder();
         while ((count = reader.read(buffer)) != -1) {
            builder.append(buffer, 0, count);
            if (size > 0)
               break;
         }
         return builder.toString();
      } catch (IOException ioe) {
         return createDefaultMessage(messageNumber);
      }
   }

   private String createDefaultMessage(int messageNumber) {
      return "test message: " + messageNumber;
   }

   public ProducerThread setMessageCount(int messageCount) {
      this.messageCount = messageCount;
      return this;
   }

   public int getSleep() {
      return sleep;
   }

   public ProducerThread setSleep(int sleep) {
      this.sleep = sleep;
      return this;
   }

   public int getMessageCount() {
      return messageCount;
   }

   public int getSentCount() {
      return sentCount.get();
   }

   public boolean isPersistent() {
      return persistent;
   }

   public ProducerThread setPersistent(boolean persistent) {
      this.persistent = persistent;
      return this;
   }

   public boolean isRunning() {
      return running;
   }

   public ProducerThread setRunning(boolean running) {
      this.running = running;
      return this;
   }

   public long getMsgTTL() {
      return msgTTL;
   }

   public ProducerThread setMsgTTL(long msgTTL) {
      this.msgTTL = msgTTL;
      return this;
   }

   public int getTransactionBatchSize() {
      return transactionBatchSize;
   }

   public ProducerThread setTransactionBatchSize(int transactionBatchSize) {
      this.transactionBatchSize = transactionBatchSize;
      return this;
   }

   public String getMsgGroupID() {
      return msgGroupID;
   }

   public ProducerThread setMsgGroupID(String msgGroupID) {
      this.msgGroupID = msgGroupID;
      return this;
   }

   public int getTextMessageSize() {
      return textMessageSize;
   }

   public ProducerThread setTextMessageSize(int textMessageSize) {
      this.textMessageSize = textMessageSize;
      return this;
   }

   public int getMessageSize() {
      return messageSize;
   }

   public ProducerThread setMessageSize(int messageSize) {
      this.messageSize = messageSize;
      return this;
   }

   public ReusableLatch getFinished() {
      return finished;
   }

   public ProducerThread setFinished(int value) {
      finished.setCount(value);
      return this;
   }

   public String getPayloadUrl() {
      return payloadUrl;
   }

   public ProducerThread setPayloadUrl(String payloadUrl) {
      this.payloadUrl = payloadUrl;
      return this;
   }

   public String getMessage() {
      return message;
   }

   public ProducerThread setMessage(String message) {
      this.message = message;
      return this;
   }

   public boolean isRunIndefinitely() {
      return runIndefinitely;
   }

   public ProducerThread setRunIndefinitely(boolean runIndefinitely) {
      this.runIndefinitely = runIndefinitely;
      return this;
   }

   public ProducerThread pauseProducer() {
      this.paused.countUp();
      return this;
   }

   public ProducerThread resumeProducer() {
      this.paused.countDown();
      return this;
   }

   public ProducerThread resetCounters() {
      this.sentCount.set(0);
      return this;
   }

   public boolean isVerbose() {
      return verbose;
   }

   public ProducerThread setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
   }
}
