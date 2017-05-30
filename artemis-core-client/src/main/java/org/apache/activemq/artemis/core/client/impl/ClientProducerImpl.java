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
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.message.LargeBodyEncoder;
import org.apache.activemq.artemis.spi.core.remoting.SessionContext;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;
import org.apache.activemq.artemis.utils.DeflaterReader;
import org.apache.activemq.artemis.utils.TokenBucketLimiter;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

/**
 * The client-side Producer.
 */
public class ClientProducerImpl implements ClientProducerInternal {

   private static final Logger logger = Logger.getLogger(ClientProducerImpl.class);

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
      } else {
         this.groupID = groupID;
      }

      this.minLargeMessageSize = minLargeMessageSize;

      if (address != null) {
         producerCredits = session.getCredits(address, false);
      } else {
         producerCredits = null;
      }
   }

   // ClientProducer implementation ----------------------------------------------------------------

   @Override
   public SimpleString getAddress() {
      return address;
   }

   @Override
   public void send(final Message msg) throws ActiveMQException {
      checkClosed();

      doSend(null, msg, null, false);
   }

   @Override
   public void send(final SimpleString address1, final Message msg) throws ActiveMQException {
      checkClosed();

      doSend(address1, msg, null, false);
   }

   @Override
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
      } else {
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

   @Override
   public synchronized void close() throws ActiveMQException {
      if (closed) {
         return;
      }

      doCleanup();
   }

   @Override
   public void cleanUp() {
      if (closed) {
         return;
      }

      doCleanup();
   }

   @Override
   public boolean isClosed() {
      return closed;
   }

   @Override
   public boolean isBlockOnDurableSend() {
      return blockOnDurableSend;
   }

   @Override
   public boolean isBlockOnNonDurableSend() {
      return blockOnNonDurableSend;
   }

   @Override
   public int getMaxRate() {
      return rateLimiter == null ? -1 : rateLimiter.getRate();
   }

   // Public ---------------------------------------------------------------------------------------

   @Override
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

   private void doSend(SimpleString sendingAddress,
                       final Message msgToSend,
                       final SendAcknowledgementHandler handler,
                       final boolean forceAsync) throws ActiveMQException {
      if (sendingAddress == null) {
         sendingAddress = this.address;
      }
      session.startCall();

      try {
         // In case we received message from another protocol, we first need to convert it to core as the ClientProducer only understands core
         ICoreMessage msg = msgToSend.toCore();

         ClientProducerCredits theCredits;

         boolean isLarge;
         // a note about the second check on the writerIndexSize,
         // If it's a server's message, it means this is being done through the bridge or some special consumer on the
         // server's on which case we can't' convert the message into large at the servers
         if (sessionContext.supportsLargeMessage() && (getBodyInputStream(msg) != null || msg.isLargeMessage() ||
            msg.getBodyBuffer().writerIndex() > minLargeMessageSize)) {
            isLarge = true;
         } else {
            isLarge = false;
         }

         if (!isLarge) {
            session.setAddress(msg, sendingAddress);
         } else {
            msg.setAddress(sendingAddress);
         }

         // Anonymous
         theCredits = session.getCredits(sendingAddress, true);

         if (rateLimiter != null) {
            // Rate flow control

            rateLimiter.limit();
         }

         if (groupID != null) {
            msg.putStringProperty(Message.HDR_GROUP_ID, groupID);
         }

         final boolean sendBlockingConfig = msg.isDurable() ? blockOnDurableSend : blockOnNonDurableSend;
         final boolean forceAsyncOverride = handler != null;
         final boolean sendBlocking = sendBlockingConfig && !forceAsyncOverride;

         session.workDone();

         if (isLarge) {
            largeMessageSend(sendBlocking, msg, theCredits, handler);
         } else {
            sendRegularMessage(sendingAddress, msg, sendBlocking, theCredits, handler);
         }
      } finally {
         session.endCall();
      }
   }

   private InputStream getBodyInputStream(ICoreMessage msgI) {
      return msgI.getBodyInputStream();
   }

   private void sendRegularMessage(final SimpleString sendingAddress,
                                   final ICoreMessage msgI,
                                   final boolean sendBlocking,
                                   final ClientProducerCredits theCredits,
                                   final SendAcknowledgementHandler handler) throws ActiveMQException {
      // This will block if credits are not available

      // Note, that for a large message, the encode size only includes the properties + headers
      // Not the continuations, but this is ok since we are only interested in limiting the amount of
      // data in *memory* and continuations go straight to the disk

      logger.tracef("sendRegularMessage::%s, Blocking=%s", msgI, sendBlocking);

      int creditSize = sessionContext.getCreditsOnSendingFull(msgI);

      theCredits.acquireCredits(creditSize);

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
                                 final ICoreMessage msgI,
                                 final ClientProducerCredits credits,
                                 SendAcknowledgementHandler handler) throws ActiveMQException {
      logger.tracef("largeMessageSend::%s, Blocking=%s", msgI, sendBlocking);

      int headerSize = msgI.getHeadersAndPropertiesEncodeSize();

      if (msgI.getHeadersAndPropertiesEncodeSize() >= minLargeMessageSize) {
         throw ActiveMQClientMessageBundle.BUNDLE.headerSizeTooBig(headerSize);
      }

      // msg.getBody() could be Null on LargeServerMessage
      if (getBodyInputStream(msgI) == null && msgI.getBuffer() != null) {
         msgI.getBuffer().readerIndex(0);
      }

      InputStream input;

      if (msgI.isServerMessage()) {
         largeMessageSendServer(sendBlocking, msgI, credits, handler);
      } else if ((input = getBodyInputStream(msgI)) != null) {
         largeMessageSendStreamed(sendBlocking, msgI, input, credits, handler);
      } else {
         largeMessageSendBuffered(sendBlocking, msgI, credits, handler);
      }
   }

   private void sendInitialLargeMessageHeader(Message msgI,
                                              ClientProducerCredits credits) throws ActiveMQException {
      int creditsUsed = sessionContext.sendInitialChunkOnLargeMessage(msgI);

      // On the case of large messages we tried to send credits before but we would starve otherwise
      // we may find a way to improve the logic and always acquire the credits before
      // but that's the way it's been tested and been working ATM
      credits.acquireCredits(creditsUsed);
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
                                       final ICoreMessage msgI,
                                       final ClientProducerCredits credits,
                                       SendAcknowledgementHandler handler) throws ActiveMQException {
      sendInitialLargeMessageHeader(msgI, credits);

      LargeBodyEncoder context = msgI.getBodyEncoder();

      final long bodySize = context.getLargeBodySize();
      context.open();
      try {

         for (long pos = 0; pos < bodySize; ) {
            final boolean lastChunk;

            final int chunkLength = (int) Math.min((bodySize - pos), minLargeMessageSize);

            final ActiveMQBuffer bodyBuffer = ActiveMQBuffers.fixedBuffer(chunkLength);

            context.encode(bodyBuffer, chunkLength);

            pos += chunkLength;

            lastChunk = pos >= bodySize;
            SendAcknowledgementHandler messageHandler = lastChunk ? handler : null;

            int creditsUsed = sessionContext.sendServerLargeMessageChunk(msgI, -1, sendBlocking, lastChunk, bodyBuffer.toByteBuffer().array(), messageHandler);

            credits.acquireCredits(creditsUsed);
         }
      } finally {
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
                                         final ICoreMessage msgI,
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
                                         final ICoreMessage msgI,
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

      int reconnectID = sessionContext.getReconnectID();
      while (!lastPacket) {
         byte[] buff = new byte[minLargeMessageSize];

         int pos = 0;

         do {
            int numberOfBytesRead;

            int wanted = minLargeMessageSize - pos;

            try {
               numberOfBytesRead = input.read(buff, pos, wanted);
            } catch (IOException e) {
               throw ActiveMQClientMessageBundle.BUNDLE.errorReadingBody(e);
            }

            if (numberOfBytesRead == -1) {
               lastPacket = true;

               break;
            }

            pos += numberOfBytesRead;
         }
         while (pos < minLargeMessageSize);

         totalSize += pos;

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
               sendRegularMessage(msgI.getAddressSimpleString(), msgI, sendBlocking, credits, handler);
               return;
            } else {
               if (!headerSent) {
                  headerSent = true;
                  sendInitialLargeMessageHeader(msgI, credits);
               }
               int creditsSent = sessionContext.sendLargeMessageChunk(msgI, messageSize.get(), sendBlocking, true, buff, reconnectID, handler);
               credits.acquireCredits(creditsSent);
            }
         } else {
            if (!headerSent) {
               headerSent = true;
               sendInitialLargeMessageHeader(msgI, credits);
            }

            int creditsSent = sessionContext.sendLargeMessageChunk(msgI, messageSize.get(), sendBlocking, false, buff, reconnectID, handler);
            credits.acquireCredits(creditsSent);
         }
      }

      try {
         input.close();
      } catch (IOException e) {
         throw ActiveMQClientMessageBundle.BUNDLE.errorClosingLargeMessage(e);
      }
   }
}
