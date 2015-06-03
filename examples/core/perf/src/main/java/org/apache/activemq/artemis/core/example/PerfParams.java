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
package org.apache.activemq.artemis.core.example;

import java.io.Serializable;

/**
 * Class that holds the parameters used in the performance examples
 */
public class PerfParams implements Serializable
{
   private static final long serialVersionUID = -4336539641012356002L;

   private int noOfMessagesToSend = 1000;

   private int noOfWarmupMessages;

   private int messageSize = 1024; // in bytes

   private boolean durable = false;

   private boolean isSessionTransacted = false;

   private int batchSize = 5000;

   private boolean drainQueue = true;

   private String queueName;

   private String address;

   private int throttleRate;

   private String host;

   private int port;

   private int tcpBufferSize;

   private boolean tcpNoDelay;

   private boolean preAck;

   private int confirmationWindow = -1;

   private int producerWindow;

   private int consumerWindow;

   private boolean blockOnPersistent = true;

   private boolean blockOnACK = true;

   private boolean useSendAcks;

   public boolean isBlockOnPersistent()
   {
      return blockOnPersistent;
   }

   public void setBlockOnPersistent(final boolean blockOnPersistent)
   {
      this.blockOnPersistent = blockOnPersistent;
   }

   public boolean isBlockOnACK()
   {
      return blockOnACK;
   }

   public void setBlockOnACK(final boolean blockOnACK)
   {
      this.blockOnACK = blockOnACK;
   }

   public int getNoOfMessagesToSend()
   {
      return noOfMessagesToSend;
   }

   public void setNoOfMessagesToSend(final int noOfMessagesToSend)
   {
      this.noOfMessagesToSend = noOfMessagesToSend;
   }

   public int getNoOfWarmupMessages()
   {
      return noOfWarmupMessages;
   }

   public void setNoOfWarmupMessages(final int noOfWarmupMessages)
   {
      this.noOfWarmupMessages = noOfWarmupMessages;
   }

   public int getMessageSize()
   {
      return messageSize;
   }

   public void setMessageSize(final int messageSize)
   {
      this.messageSize = messageSize;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   public boolean isSessionTransacted()
   {
      return isSessionTransacted;
   }

   public void setSessionTransacted(final boolean sessionTransacted)
   {
      isSessionTransacted = sessionTransacted;
   }

   public int getBatchSize()
   {
      return batchSize;
   }

   public void setBatchSize(final int batchSize)
   {
      this.batchSize = batchSize;
   }

   public boolean isDrainQueue()
   {
      return drainQueue;
   }

   public void setDrainQueue(final boolean drainQueue)
   {
      this.drainQueue = drainQueue;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public void setQueueName(final String queueName)
   {
      this.queueName = queueName;
   }

   public String getAddress()
   {
      return address;
   }

   public void setAddress(final String address)
   {
      this.address = address;
   }

   public int getThrottleRate()
   {
      return throttleRate;
   }

   public void setThrottleRate(final int throttleRate)
   {
      this.throttleRate = throttleRate;
   }

   @Override
   public String toString()
   {
      return "message to send = " + noOfMessagesToSend +
             ", Durable = " +
             durable +
             ", session transacted = " +
             isSessionTransacted +
             (isSessionTransacted ? ", transaction batch size = " + batchSize : "") +
             ", drain queue = " +
             drainQueue +
             ", queue name = " +
             queueName +
             ", Throttle rate = " +
             throttleRate +
             ", blockOnPersistent = " +
             blockOnPersistent +
             ". blockOnACK = " +
             blockOnACK;
   }

   public synchronized String getHost()
   {
      return host;
   }

   public synchronized void setHost(final String host)
   {
      this.host = host;
   }

   public synchronized int getPort()
   {
      return port;
   }

   public synchronized void setPort(final int port)
   {
      this.port = port;
   }

   public synchronized int getTcpBufferSize()
   {
      return tcpBufferSize;
   }

   public synchronized void setTcpBufferSize(final int tcpBufferSize)
   {
      this.tcpBufferSize = tcpBufferSize;
   }

   public synchronized boolean isTcpNoDelay()
   {
      return tcpNoDelay;
   }

   public synchronized void setTcpNoDelay(final boolean tcpNoDelay)
   {
      this.tcpNoDelay = tcpNoDelay;
   }

   public synchronized boolean isPreAck()
   {
      return preAck;
   }

   public synchronized void setPreAck(final boolean preAck)
   {
      this.preAck = preAck;
   }

   public synchronized int getConfirmationWindow()
   {
      return confirmationWindow;
   }

   public synchronized void setConfirmationWindow(final int confirmationWindow)
   {
      this.confirmationWindow = confirmationWindow;
   }

   public int getProducerWindow()
   {
      return producerWindow;
   }

   public void setProducerWindow(final int producerWindow)
   {
      this.producerWindow = producerWindow;
   }

   public int getConsumerWindow()
   {
      return consumerWindow;
   }

   public void setConsumerWindow(final int consumerWindow)
   {
      this.consumerWindow = consumerWindow;
   }

   public boolean isUseSendAcks()
   {
      return useSendAcks;
   }

   public void setUseSendAcks(boolean useSendAcks)
   {
      this.useSendAcks = useSendAcks;
   }
}
