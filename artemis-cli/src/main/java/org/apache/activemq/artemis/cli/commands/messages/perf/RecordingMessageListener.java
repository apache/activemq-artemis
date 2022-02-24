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
package org.apache.activemq.artemis.cli.commands.messages.perf;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.SingleWriterRecorder;

public final class RecordingMessageListener implements MessageListener {

   private final long id;
   private final Destination destination;
   private final boolean transaction;
   private final AtomicLong receivedMessages;
   private final Runnable onMessageReceived;
   private final MicrosTimeProvider timeProvider;
   private final SingleWriterRecorder receiveLatencyRecorder;
   private AtomicBoolean fatalException;

   RecordingMessageListener(final long id,
                            final Destination destination,
                            final boolean transaction,
                            final AtomicLong receivedMessages,
                            final Runnable onMessageReceived,
                            final MicrosTimeProvider timeProvider,
                            final SingleWriterRecorder receiveLatencyRecorder,
                            final AtomicBoolean fatalException) {
      this.id = id;
      this.destination = destination;
      this.transaction = transaction;
      this.receivedMessages = receivedMessages;
      this.onMessageReceived = onMessageReceived;
      this.timeProvider = timeProvider;
      this.receiveLatencyRecorder = receiveLatencyRecorder;
      this.fatalException = fatalException;
   }

   public boolean anyFatalException() {
      return fatalException.get();
   }

   public SingleWriterRecorder getReceiveLatencyRecorder() {
      return receiveLatencyRecorder;
   }

   public long getId() {
      return id;
   }

   public Destination getDestination() {
      return destination;
   }

   public long getReceivedMessages() {
      return receivedMessages.get();
   }

   @Override
   public void onMessage(final Message message) {
      if (onMessageReceived != null) {
         onMessageReceived.run();
      }
      receivedMessages.lazySet(receivedMessages.get() + 1);
      if (receiveLatencyRecorder != null) {
         try {
            final long start = message.getLongProperty("time");
            final long receivedOn = timeProvider.now();
            final long elapsedUs = receivedOn - start;
            receiveLatencyRecorder.recordValue(elapsedUs);
         } catch (JMSException fatal) {
            fatalException.compareAndSet(false, true);
         }
      }
      if (transaction) {
         try {
            message.acknowledge();
         } catch (JMSException fatal) {
            fatalException.compareAndSet(false, true);
         }
      }
   }
}