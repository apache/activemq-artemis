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
package org.apache.activemq6.core.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.core.client.MessageHandler;
import org.apache.activemq6.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq6.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq6.utils.TokenBucketLimiter;
import org.apache.activemq6.utils.TokenBucketLimiterImpl;

/**
 *
 * A PerfBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public abstract class PerfBase
{
   private static final Logger log = Logger.getLogger(PerfSender.class.getName());

   private static final String DEFAULT_PERF_PROPERTIES_FILE_NAME = "perf.properties";

   private static byte[] randomByteArray(final int length)
   {
      byte[] bytes = new byte[length];

      Random random = new Random();

      for (int i = 0; i < length; i++)
      {
         bytes[i] = Integer.valueOf(random.nextInt()).byteValue();
      }

      return bytes;
   }

   protected static String getPerfFileName(final String[] args)
   {
      String fileName;

      if (args.length > 0)
      {
         fileName = args[0];
      }
      else
      {
         fileName = PerfBase.DEFAULT_PERF_PROPERTIES_FILE_NAME;
      }

      PerfBase.log.info("Using file name " + fileName);

      return fileName;
   }

   protected static PerfParams getParams(final String fileName) throws Exception
   {
      Properties props = null;

      InputStream is = null;

      try
      {
         is = new FileInputStream(fileName);

         props = new Properties();

         props.load(is);
      }
      finally
      {
         if (is != null)
         {
            is.close();
         }
      }

      int noOfMessages = Integer.valueOf(props.getProperty("num-messages"));
      int noOfWarmupMessages = Integer.valueOf(props.getProperty("num-warmup-messages"));
      int messageSize = Integer.valueOf(props.getProperty("message-size"));
      boolean durable = Boolean.valueOf(props.getProperty("durable"));
      boolean transacted = Boolean.valueOf(props.getProperty("transacted"));
      int batchSize = Integer.valueOf(props.getProperty("batch-size"));
      boolean drainQueue = Boolean.valueOf(props.getProperty("drain-queue"));
      String queueName = props.getProperty("queue-name");
      String address = props.getProperty("address");
      int throttleRate = Integer.valueOf(props.getProperty("throttle-rate"));
      String host = props.getProperty("host");
      int port = Integer.valueOf(props.getProperty("port"));
      int tcpBufferSize = Integer.valueOf(props.getProperty("tcp-buffer"));
      boolean tcpNoDelay = Boolean.valueOf(props.getProperty("tcp-no-delay"));
      boolean preAck = Boolean.valueOf(props.getProperty("pre-ack"));
      int confirmationWindowSize = Integer.valueOf(props.getProperty("confirmation-window"));
      int producerWindowSize = Integer.valueOf(props.getProperty("producer-window"));
      int consumerWindowSize = Integer.valueOf(props.getProperty("consumer-window"));
      boolean blockOnACK = Boolean.valueOf(props.getProperty("block-ack", "false"));
      boolean blockOnPersistent = Boolean.valueOf(props.getProperty("block-persistent", "false"));
      boolean useSendAcks = Boolean.valueOf(props.getProperty("use-send-acks", "false"));

      PerfBase.log.info("num-messages: " + noOfMessages);
      PerfBase.log.info("num-warmup-messages: " + noOfWarmupMessages);
      PerfBase.log.info("message-size: " + messageSize);
      PerfBase.log.info("durable: " + durable);
      PerfBase.log.info("transacted: " + transacted);
      PerfBase.log.info("batch-size: " + batchSize);
      PerfBase.log.info("drain-queue: " + drainQueue);
      PerfBase.log.info("address: " + address);
      PerfBase.log.info("queue name: " + queueName);
      PerfBase.log.info("throttle-rate: " + throttleRate);
      PerfBase.log.info("host:" + host);
      PerfBase.log.info("port: " + port);
      PerfBase.log.info("tcp buffer: " + tcpBufferSize);
      PerfBase.log.info("tcp no delay: " + tcpNoDelay);
      PerfBase.log.info("pre-ack: " + preAck);
      PerfBase.log.info("confirmation-window: " + confirmationWindowSize);
      PerfBase.log.info("producer-window: " + producerWindowSize);
      PerfBase.log.info("consumer-window: " + consumerWindowSize);
      PerfBase.log.info("block-ack:" + blockOnACK);
      PerfBase.log.info("block-persistent:" + blockOnPersistent);
      PerfBase.log.info("use-send-acks:" + useSendAcks);

      if (useSendAcks && confirmationWindowSize < 1)
      {
         throw new IllegalArgumentException("If you use send acks, then need to set confirmation-window-size to a positive integer");
      }

      PerfParams perfParams = new PerfParams();
      perfParams.setNoOfMessagesToSend(noOfMessages);
      perfParams.setNoOfWarmupMessages(noOfWarmupMessages);
      perfParams.setMessageSize(messageSize);
      perfParams.setDurable(durable);
      perfParams.setSessionTransacted(transacted);
      perfParams.setBatchSize(batchSize);
      perfParams.setDrainQueue(drainQueue);
      perfParams.setQueueName(queueName);
      perfParams.setAddress(address);
      perfParams.setThrottleRate(throttleRate);
      perfParams.setHost(host);
      perfParams.setPort(port);
      perfParams.setTcpBufferSize(tcpBufferSize);
      perfParams.setTcpNoDelay(tcpNoDelay);
      perfParams.setPreAck(preAck);
      perfParams.setConfirmationWindow(confirmationWindowSize);
      perfParams.setProducerWindow(producerWindowSize);
      perfParams.setConsumerWindow(consumerWindowSize);
      perfParams.setBlockOnACK(blockOnACK);
      perfParams.setBlockOnPersistent(blockOnPersistent);
      perfParams.setUseSendAcks(useSendAcks);

      return perfParams;
   }

   private final PerfParams perfParams;

   protected PerfBase(final PerfParams perfParams)
   {
      this.perfParams = perfParams;
   }

   private ClientSessionFactory factory;

   private long start;

   private void init(final boolean transacted, final String queueName) throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(TransportConstants.TCP_NODELAY_PROPNAME, perfParams.isTcpNoDelay());
      params.put(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, perfParams.getTcpBufferSize());
      params.put(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME, perfParams.getTcpBufferSize());

      params.put(TransportConstants.HOST_PROP_NAME, perfParams.getHost());
      params.put(TransportConstants.PORT_PROP_NAME, perfParams.getPort());

      ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), params));
      serverLocator.setPreAcknowledge(perfParams.isPreAck());
      serverLocator.setConfirmationWindowSize(perfParams.getConfirmationWindow());
      serverLocator.setProducerWindowSize(perfParams.getProducerWindow());
      serverLocator.setConsumerWindowSize(perfParams.getConsumerWindow());
      serverLocator.setAckBatchSize(perfParams.getBatchSize());

      serverLocator.setBlockOnAcknowledge(perfParams.isBlockOnACK());
      serverLocator.setBlockOnDurableSend(perfParams.isBlockOnPersistent());
      factory = serverLocator.createSessionFactory();

   }

   private void displayAverage(final long numberOfMessages, final long start, final long end)
   {
      double duration = (1.0 * end - start) / 1000; // in seconds
      double average = 1.0 * numberOfMessages / duration;
      PerfBase.log.info(String.format("average: %.2f msg/s (%d messages in %2.2fs)",
                                      average,
                                      numberOfMessages,
                                      duration));
   }

   protected void runSender()
   {
      try
      {
         PerfBase.log.info("params = " + perfParams);

         init(perfParams.isSessionTransacted(), perfParams.getQueueName());

         if (perfParams.isDrainQueue())
         {
            drainQueue();
         }

         start = System.currentTimeMillis();
         PerfBase.log.info("warming up by sending " + perfParams.getNoOfWarmupMessages() + " messages");
         sendMessages(perfParams.getNoOfWarmupMessages(),
                      perfParams.getBatchSize(),
                      perfParams.isDurable(),
                      perfParams.isSessionTransacted(),
                      false,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize(),
                      perfParams.isUseSendAcks());
         PerfBase.log.info("warmed up");
         start = System.currentTimeMillis();
         sendMessages(perfParams.getNoOfMessagesToSend(),
                      perfParams.getBatchSize(),
                      perfParams.isDurable(),
                      perfParams.isSessionTransacted(),
                      true,
                      perfParams.getThrottleRate(),
                      perfParams.getMessageSize(),
                      perfParams.isUseSendAcks());
         long end = System.currentTimeMillis();
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   protected void runListener()
   {
      ClientSession session = null;

      try
      {
         init(perfParams.isSessionTransacted(), perfParams.getQueueName());

         session = factory.createSession(!perfParams.isSessionTransacted(), !perfParams.isSessionTransacted());

         if (perfParams.isDrainQueue())
         {
            drainQueue();
         }

         ClientConsumer consumer = session.createConsumer(perfParams.getQueueName());

         session.start();

         PerfBase.log.info("READY!!!");

         CountDownLatch countDownLatch = new CountDownLatch(1);
         consumer.setMessageHandler(new PerfListener(session, countDownLatch, perfParams));
         countDownLatch.await();
         long end = System.currentTimeMillis();
         // start was set on the first received message
         displayAverage(perfParams.getNoOfMessagesToSend(), start, end);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (factory != null)
         {
            try
            {
               factory.close();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   private void drainQueue() throws Exception
   {
      PerfBase.log.info("Draining queue");

      ClientSession session = null;

      try
      {
         session = factory.createSession();

         ClientConsumer consumer = session.createConsumer(perfParams.getQueueName());

         session.start();

         ClientMessage message = null;

         int count = 0;
         do
         {
            message = consumer.receive(3000);

            if (message != null)
            {
               message.acknowledge();

               count++;
            }
         }
         while (message != null);

         PerfBase.log.info("Drained " + count + " messages");
      }
      finally
      {
         if (session != null)
         {
            session.close();
         }
      }
   }

   private void sendMessages(final int numberOfMessages,
                             final int txBatchSize,
                             final boolean durable,
                             final boolean transacted,
                             final boolean display,
                             final int throttleRate,
                             final int messageSize,
                             final boolean useSendAcks) throws Exception
   {
      ClientSession session = null;

      try
      {
         session = factory.createSession(!transacted, !transacted);

         CountDownLatch theLatch = null;

         if (useSendAcks)
         {
            final CountDownLatch latch = new CountDownLatch(numberOfMessages);

            class MySendAckHandler implements SendAcknowledgementHandler
            {
               public void sendAcknowledged(Message message)
               {
                  latch.countDown();
               }
            }

            session.setSendAcknowledgementHandler(new MySendAckHandler());

            theLatch = latch;
         }

         ClientProducer producer = session.createProducer(perfParams.getAddress());

         ClientMessage message = session.createMessage(durable);

         byte[] payload = PerfBase.randomByteArray(messageSize);

         message.getBodyBuffer().writeBytes(payload);

         final int modulo = 2000;

         TokenBucketLimiter tbl = throttleRate != -1 ? new TokenBucketLimiterImpl(throttleRate, false) : null;

         boolean committed = false;

         for (int i = 1; i <= numberOfMessages; i++)
         {
            producer.send(message);

            if (transacted)
            {
               if (i % txBatchSize == 0)
               {
                  session.commit();
                  committed = true;
               }
               else
               {
                  committed = false;
               }
            }
            if (display && i % modulo == 0)
            {
               double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
               PerfBase.log.info(String.format("sent %6d messages in %2.2fs", i, duration));
            }

            // log.info("sent message " + i);

            if (tbl != null)
            {
               tbl.limit();
            }
         }

         if (transacted && !committed)
         {
            session.commit();
         }

         session.close();

         if (useSendAcks)
         {
            theLatch.await();
         }
      }
      finally
      {
         if (session != null)
         {
            session.close();
         }
      }
   }

   private class PerfListener implements MessageHandler
   {
      private final CountDownLatch countDownLatch;

      private final PerfParams perfParams;

      private boolean warmingUp = true;

      private boolean started = false;

      private final int modulo;

      private final AtomicLong count = new AtomicLong(0);

      private final ClientSession session;

      public PerfListener(final ClientSession session, final CountDownLatch countDownLatch, final PerfParams perfParams)
      {
         this.session = session;
         this.countDownLatch = countDownLatch;
         this.perfParams = perfParams;
         warmingUp = perfParams.getNoOfWarmupMessages() > 0;
         modulo = 2000;
      }

      public void onMessage(final ClientMessage message)
      {
         try
         {
            if (warmingUp)
            {
               boolean committed = checkCommit(session);
               if (count.incrementAndGet() == perfParams.getNoOfWarmupMessages())
               {
                  PerfBase.log.info("warmed up after receiving " + count.longValue() + " msgs");
                  if (!committed)
                  {
                     checkCommit(session);
                  }
                  warmingUp = false;
               }
               return;
            }

            if (!started)
            {
               started = true;
               // reset count to take stats
               count.set(0);
               start = System.currentTimeMillis();
            }

            message.acknowledge();

            long currentCount = count.incrementAndGet();
            boolean committed = checkCommit(session);
            if (currentCount == perfParams.getNoOfMessagesToSend())
            {
               if (!committed)
               {
                  checkCommit(session);
               }
               countDownLatch.countDown();
            }
            if (currentCount % modulo == 0)
            {
               double duration = (1.0 * System.currentTimeMillis() - start) / 1000;
               PerfBase.log.info(String.format("received %6d messages in %2.2fs", currentCount, duration));
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      private boolean checkCommit(final ClientSession session) throws Exception
      {
         if (perfParams.isSessionTransacted())
         {
            if (count.longValue() % perfParams.getBatchSize() == 0)
            {
               session.commit();

               return true;
            }
         }
         return false;
      }
   }

}
