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

package org.apache.activemq.artemis.protocol.amqp.proton;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;

/**
 * Reader of {@link AMQPLargeMessage} content which reads all bytes and completes once a
 * non-partial delivery is read.
 */
public class AMQPLargeMessageReader implements MessageReader {

   private final ProtonAbstractReceiver serverReceiver;

   private AMQPLargeMessage currentMessage;
   private DeliveryAnnotations deliveryAnnotations;
   private boolean closed = true;

   public AMQPLargeMessageReader(ProtonAbstractReceiver serverReceiver) {
      this.serverReceiver = serverReceiver;
   }

   @Override
   public DeliveryAnnotations getDeliveryAnnotations() {
      return deliveryAnnotations;
   }

   @Override
   public void close() {
      if (!closed) {
         if (currentMessage != null) {
            try {
               currentMessage.deleteFile();
            } catch (Throwable error) {
               ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
            } finally {
               currentMessage = null;
            }
         }

         deliveryAnnotations = null;
         closed = true;
      }
   }

   @Override
   public AMQPLargeMessageReader open() {
      if (!closed) {
         throw new IllegalStateException("Reader was not closed before call to open.");
      }

      closed = false;

      return this;
   }

   @Override
   public Message readBytes(Delivery delivery) throws Exception {
      if (closed) {
         throw new IllegalStateException("AMQP Large Message Reader is closed and read cannot proceed");
      }

      final Receiver receiver = ((Receiver) delivery.getLink());
      final ReadableBuffer dataBuffer = receiver.recv();

      if (currentMessage == null) {
         final AMQPSessionCallback sessionSPI = serverReceiver.getSessionContext().getSessionSPI();
         final long id = sessionSPI.getStorageManager().generateID();
         currentMessage = new AMQPLargeMessage(id, delivery.getMessageFormat(), null,
                                               sessionSPI.getCoreMessageObjectPools(),
                                               sessionSPI.getStorageManager());
         currentMessage.parseHeader(dataBuffer);

         sessionSPI.getStorageManager().largeMessageCreated(id, currentMessage);
      }

      currentMessage.addBytes(dataBuffer);

      final AMQPLargeMessage result;

      if (!delivery.isPartial()) {
         currentMessage.releaseResources(serverReceiver.getConnection().isLargeMessageSync(), true);
         result = currentMessage;
         // We don't want a close to delete the file now, we've released the resources.
         currentMessage = null;
         deliveryAnnotations = result.getDeliveryAnnotations();
      } else {
         result = null;
      }

      return result;
   }
}
