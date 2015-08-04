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
package org.apache.activemq.artemis.core.client.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendContinuationMessage;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;
import org.apache.activemq.artemis.utils.DeflaterReader;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;
import org.apache.activemq.artemis.utils.TokenBucketLimiter;
import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * The client-side Producer.
 */
public class ClientProducerImpl implements ClientProducerInternal {

   private final SimpleString address;

   private final ClientSessionInternal session;

   private final SessionContext sessionContext;

   private volatile boolean closed;

   // For rate throttling

   private final TokenBucketLimiter rateLimiter;

   private final boolean blockOnNonDurableSend;

   private final boolean blockOnDurableSend;

   private final SimpleString groupID;

   private final int minLargeMessageSize;

   private final ClientProducerCredits producerCredits;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientProducerImpl(final ClientSessionInternal session,
                             final SimpleString address,
                             final TokenBucketLimiter rateLimiter,
                             final boolean blockOnNonDurableSend,
                             final boolean blockOnDurableSend,
                             final boolean autoGroup,
                             final SimpleString groupID,
                             final int minLargeMessageSize,
                             final SessionContext sessionContext) {
      this.sessionContext = sessionContext;

      this.session = session;

      this.address = address;

      this.rateLimiter = rateLimiter;

      this.blockOnNonDurableSend = blockOnNonDurableSend;

      this.blockOnDurableSend = blockOnDurableSend;

      if (autoGroup) {
         this.groupID = UUIDGenerator.getInstance().generateSimpleStringUUID();
      }
      else {
         this.groupID = groupID;
      }

      this.minLargeMessageSize = minLargeMessageSize;

      if (address != null) {
         producerCredits = session.getCredits(address, false);
      }
      else {
         producerCredits = null;
      }
   }

   // ClientProducer implementation ----------------------------------------------------------------

   public SimpleString getAddress() {
      return address;
   }

   public void send(final Message msg) throws ActiveMQException {
      checkClosed();

      doSend(null, msg, null, false);
   }

   public void send(final SimpleString address1, final Message msg) throws ActiveMQException {
      checkClosed();

      doSend(address1, msg, null, false);
   }

   public void send(final String address1, final Message message) throws ActiveMQException {
      send(SimpleString.toSimpleString(address1), message);
   }

   @Override
   public void send(SimpleString address1,
                    Message message,
                    SendAcknowledgementHandler handler) throws ActiveMQException {
      checkClosed();
      boolean confirmationWindowEnabled = session.isConfirmationWindowEnabled();
      if (confirmationWindowEnabled) {
         doSend(address1, message, handler, true);
      }
      else {
         doSend(address1, message, null, true);
         if (handler != null) {
            session.scheduleConfirmation(handler, message);
         }
      }
   }

   @Override
   public void send(Message message, SendAcknowledgementHandler handler) throws ActiveMQException {
      send(null, message, handler);
   }

   public synchronized void close() throws ActiveMQException {
      if (closed) {
         return;
      }

      doCleanup();
   }

   public void cleanUp() {
      if (closed) {
         return;
      }

      doCleanup();
   }

   public boolean isClosed() {
      return closed;
   }

   public boolean isBlockOnDurableSend() {
      return blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend() {
      return blockOnNonDurableSend;
   }

   public int getMaxRate() {
      return rateLimiter == null ? -1 : rateLimiter.getRate();
   }

   // Public ---------------------------------------------------------------------------------------

   public ClientProducerCredits getProducerCredits() {
      return producerCredits;
   }

   private void doCleanup() {
      if (address != null) {
         session.returnCredits(address);
      }

      session.removeProducer(this);

      closed = true;
   }

   private void doSend(final SimpleString address1,
                       final Message msg,
                       final SendAcknowledgementHandler handler,
                       final boolean forceAsync) throws ActiveMQException {
      session.startCall();

      try {
         MessageInternal msgI = (MessageInternal) msg;

         ClientProducerCredits theCredits;

         boolean isLarge;
         // a note about the second check on the writerIndexSize,
         // If it's a server's message, it means this is being done through the bridge or some special consumer on the
         // server's on which case we can't' convert the message into large at the servers
         if (sessionContext.supportsLargeMessage() && (msgI.getBodyInputStream() != null || msgI.isLargeMessage() ||
            msgI.getBodyBuffer().writerIndex() > minLargeMessageSize && !msgI.isServerMessage())) {
            isLarge = true;
         }
         else {
            isLarge = false;
         }

         if (address1 != null) {
            if (!isLarge) {
               session.setAddress(msg, address1);
            }
            else {
               msg.setAddress(address1);
            }

            // Anonymous
            theCredits = session.getCredits(address1, true);
         }
         else {
            if (!isLarge) {
               session.setAddress(msg, this.address);
            }
            else {
               msg.setAddress(this.address);
            }

            theCredits = producerCredits;
         }

         if (rateLimiter != null) {
            // Rate flow control

            rateLimiter.limit();
         }

         if (groupID != null) {
            msgI.putStringProperty(Message.HDR_GROUP_ID, groupID);
         }

         final boolean sendBlockingConfig = msgI.isDurable() ? blockOnDurableSend : blockOnNonDurableSend;
         final boolean forceAsyncOverride = handler != null;
         final boolean sendBlocking = sendBlockingConfig && !forceAsyncOverride;

         session.workDone();

         if (isLarge) {
            largeMessageSend(sendBlocking, msgI, theCredits, handler);
         }
         else {
            sendRegularMessage(msgI, sendBlocking, theCredits, handler);
         }
      }
      finally {
         session.endCall();
      }
   }

   private void sendRegularMessage(final MessageInternal msgI,
                                   final boolean sendBlocking,
                                   final ClientProducerCredits theCredits,
                                   final SendAcknowledgementHandler handler) throws ActiveMQException {
      try {
         // This will block if credits are not available

         // Note, that for a large message, the encode size only includes the properties + headers
         // Not the continuations, but this is ok since we are only interested in limiting the amount of
         // data in *memory* and continuations go straight to the disk

         int creditSize = sessionContext.getCreditsOnSendingFull(msgI);

         theCredits.acquireCredits(creditSize);
      }
      catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }

      sessionContext.sendFullMessage(msgI, sendBlocking, handler, address);
   }

   private void checkClosed() throws ActiveMQException {
      if (closed) {
         throw ActiveMQClientMessageBundle.BUNDLE.producerClosed();
      }
   }

   // Methods to send Large Messages----------------------------------------------------------------

   /**
    * @param msgI
    * @param handler
    * @throws ActiveMQException
    */
   private void largeMessageSend(final boolean sendBlocking,
                                 final MessageInternal msgI,
                                 final ClientProducerCredits credits,
                                 SendAcknowledgementHandler handler) throws ActiveMQException {
      int headerSize = msgI.getHeadersAndPropertiesEncodeSize();

      if (msgI.getHeadersAndPropertiesEncodeSize() >= minLargeMessageSize) {
         throw ActiveMQClientMessageBundle.BUNDLE.headerSizeTooBig(headerSize);
      }

      // msg.getBody() could be Null on LargeServerMessage
      if (msgI.getBodyInputStream() == null && msgI.getWholeBuffer() != null) {
         msgI.getWholeBuffer().readerIndex(0);
      }

      InputStream input;

      if (msgI.isServerMessage()) {
         largeMessageSendServer(sendBlocking, msgI, credits, handler);
      }
      else if ((input = msgI.getBodyInputStream()) != null) {
         largeMessageSendStreamed(sendBlocking, msgI, input, credits, handler);
      }
      else {
         largeMessageSendBuffered(sendBlocking, msgI, credits, handler);
      }
   }

   private void sendInitialLargeMessageHeader(MessageInternal msgI,
                                              ClientProducerCredits credits) throws ActiveMQException {
      int creditsUsed = sessionContext.sendInitialChunkOnLargeMessage(msgI);

      // On the case of large messages we tried to send credits before but we would starve otherwise
      // we may find a way to improve the logic and always acquire the credits before
      // but that's the way it's been tested and been working ATM
      try {
         credits.acquireCredits(creditsUsed);
      }
      catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }
   }

   /**
    * Used to send serverMessages through the bridges. No need to validate compression here since
    * the message is only compressed at the client
    *
    * @param sendBlocking
    * @param msgI
    * @param handler
    * @throws ActiveMQException
    */
   private void largeMessageSendServer(final boolean sendBlocking,
                                       final MessageInternal msgI,
                                       final ClientProducerCredits credits,
                                       SendAcknowledgementHandler handler) throws ActiveMQException {
      sendInitialLargeMessageHeader(msgI, credits);

      BodyEncoder context = msgI.getBodyEncoder();

      final long bodySize = context.getLargeBodySize();

      context.open();
      try {

         for (int pos = 0; pos < bodySize; ) {
            final boolean lastChunk;

            final int chunkLength = Math.min((int) (bodySize - pos), minLargeMessageSize);

            final ActiveMQBuffer bodyBuffer = ActiveMQBuffers.fixedBuffer(chunkLength);

            context.encode(bodyBuffer, chunkLength);

            pos += chunkLength;

            lastChunk = pos >= bodySize;
            SendAcknowledgementHandler messageHandler = lastChunk ? handler : null;

            int creditsUsed = sessionContext.sendLargeMessageChunk(msgI, -1, sendBlocking, lastChunk, bodyBuffer.toByteBuffer().array(), messageHandler);

            try {
               credits.acquireCredits(creditsUsed);
            }
            catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }
      finally {
         context.close();
      }
   }

   /**
    * @param sendBlocking
    * @param msgI
    * @param handler
    * @throws ActiveMQException
    */
   private void largeMessageSendBuffered(final boolean sendBlocking,
                                         final MessageInternal msgI,
                                         final ClientProducerCredits credits,
                                         SendAcknowledgementHandler handler) throws ActiveMQException {
      msgI.getBodyBuffer().readerIndex(0);
      largeMessageSendStreamed(sendBlocking, msgI, new ActiveMQBufferInputStream(msgI.getBodyBuffer()), credits, handler);
   }

   /**
    * @param sendBlocking
    * @param msgI
    * @param inputStreamParameter
    * @param credits
    * @throws ActiveMQException
    */
   private void largeMessageSendStreamed(final boolean sendBlocking,
                                         final MessageInternal msgI,
                                         final InputStream inputStreamParameter,
                                         final ClientProducerCredits credits,
                                         SendAcknowledgementHandler handler) throws ActiveMQException {
      boolean lastPacket = false;

      InputStream input = inputStreamParameter;

      // We won't know the real size of the message since we are compressing while reading the streaming.
      // This counter will be passed to the deflater to be updated for every byte read
      AtomicLong messageSize = new AtomicLong();

      DeflaterReader deflaterReader = null;

      if (session.isCompressLargeMessages()) {
         msgI.putBooleanProperty(Message.HDR_LARGE_COMPRESSED, true);
         deflaterReader = new DeflaterReader(inputStreamParameter, messageSize);
         input = deflaterReader;
      }

      long totalSize = 0;

      boolean headerSent = false;

      while (!lastPacket) {
         byte[] buff = new byte[minLargeMessageSize];

         int pos = 0;

         do {
            int numberOfBytesRead;

            int wanted = minLargeMessageSize - pos;

            try {
               numberOfBytesRead = input.read(buff, pos, wanted);
            }
            catch (IOException e) {
               throw ActiveMQClientMessageBundle.BUNDLE.errorReadingBody(e);
            }

            if (numberOfBytesRead == -1) {
               lastPacket = true;

               break;
            }

            pos += numberOfBytesRead;
         } while (pos < minLargeMessageSize);

         totalSize += pos;

         final SessionSendContinuationMessage chunk;

         if (lastPacket) {
            if (!session.isCompressLargeMessages()) {
               messageSize.set(totalSize);
            }

            // This is replacing the last packet by a smaller packet
            byte[] buff2 = new byte[pos];

            System.arraycopy(buff, 0, buff2, 0, pos);

            buff = buff2;

            // This is the case where the message is being converted as a regular message
            if (!headerSent && session.isCompressLargeMessages() && buff2.length < minLargeMessageSize) {
               msgI.getBodyBuffer().resetReaderIndex();
               msgI.getBodyBuffer().resetWriterIndex();
               msgI.putLongProperty(Message.HDR_LARGE_BODY_SIZE, deflaterReader.getTotalSize());

               msgI.getBodyBuffer().writeBytes(buff, 0, pos);
               sendRegularMessage(msgI, sendBlocking, credits, handler);
               return;
            }
            else {
               if (!headerSent) {
                  headerSent = true;
                  sendInitialLargeMessageHeader(msgI, credits);
               }
               int creditsSent = sessionContext.sendLargeMessageChunk(msgI, messageSize.get(), sendBlocking, true, buff, handler);
               try {
                  credits.acquireCredits(creditsSent);
               }
               catch (InterruptedException e) {
                  throw new ActiveMQInterruptedException(e);
               }
            }
         }
         else {
            if (!headerSent) {
               headerSent = true;
               sendInitialLargeMessageHeader(msgI, credits);
            }

            int creditsSent = sessionContext.sendLargeMessageChunk(msgI, messageSize.get(), sendBlocking, false, buff, handler);
            try {
               credits.acquireCredits(creditsSent);
            }
            catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }

      try {
         input.close();
      }
      catch (IOException e) {
         throw ActiveMQClientMessageBundle.BUNDLE.errorClosingLargeMessage(e);
      }
   }
}
