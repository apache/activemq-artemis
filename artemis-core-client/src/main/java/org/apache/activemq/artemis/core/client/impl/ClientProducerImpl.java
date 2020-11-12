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
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQLargeMessageException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
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

      send(null, msg, sessionContext.getSendAcknowledgementHandler());
   }

   @Override
   public void send(final SimpleString address1, final Message msg) throws ActiveMQException {
      checkClosed();

      send(address1, msg, sessionContext.getSendAcknowledgementHandler());
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

      if (handler != null) {
         handler = new SendAcknowledgementHandlerWrapper(handler);
      }

      doSend(address1, message, handler);

      if (handler != null && !session.isConfirmationWindowEnabled()) {
         if (logger.isDebugEnabled()) {
            logger.debug("Handler was used on producing messages towards address " + address1 + " however there is no confirmationWindowEnabled");
         }

         // if there is no confirmation enabled, we will at least call the handler after the sent is done
         session.scheduleConfirmation(handler, message);
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
                       final SendAcknowledgementHandler handler) throws ActiveMQException {
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
         // if Handler != null, we will send non blocking
         final boolean sendBlocking = sendBlockingConfig && handler == null && sessionContext.getSendAcknowledgementHandler() == null;

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

      LargeBodyReader context = msgI.getLargeBodyReader();

      final long bodySize = context.getSize();
      context.open();
      try {

         for (long pos = 0; pos < bodySize; ) {
            final boolean lastChunk;

            final int chunkLength = (int) Math.min((bodySize - pos), minLargeMessageSize);

            final ByteBuffer bodyBuffer = ByteBuffer.allocate(chunkLength);

            final int encodedSize = context.readInto(bodyBuffer);

            assert encodedSize == chunkLength;

            pos += chunkLength;

            lastChunk = pos >= bodySize;
            SendAcknowledgementHandler messageHandler = lastChunk ? handler : null;

            int creditsUsed = sessionContext.sendServerLargeMessageChunk(msgI, -1, sendBlocking, lastChunk, bodyBuffer.array(), messageHandler);

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
    * @param inputStream
    * @param credits
    * @throws ActiveMQException
    */
   private void largeMessageSendStreamed(final boolean sendBlocking,
                                         final ICoreMessage msgI,
                                         final InputStream inputStream,
                                         final ClientProducerCredits credits,
                                         SendAcknowledgementHandler handler) throws ActiveMQException {
      final DeflaterReader deflaterReader = session.isCompressLargeMessages() ? new DeflaterReader(inputStream) : null;

      try (InputStream input = session.isCompressLargeMessages() ? deflaterReader : inputStream) {

         if (session.isCompressLargeMessages()) {
            msgI.putBooleanProperty(Message.HDR_LARGE_COMPRESSED, true);
         }

         long totalBytesRead = 0;

         boolean headerSent = false;

         int reconnectID = sessionContext.getReconnectID();
         while (true) {
            final byte[] scratchBuffer = new byte[minLargeMessageSize];

            final int result = readLargeMessageChunk(input, scratchBuffer, minLargeMessageSize);

            final int bytesRead = bytesRead(result);

            totalBytesRead += bytesRead;

            final boolean lastPacket = isLargeMessageLastPacket(result);

            if (lastPacket) {
               // This is the case where the message is being converted as a regular message
               if (!headerSent && session.isCompressLargeMessages() && bytesRead < minLargeMessageSize) {
                  assert bytesRead == totalBytesRead;
                  msgI.getBodyBuffer().resetReaderIndex();
                  msgI.getBodyBuffer().resetWriterIndex();
                  msgI.putLongProperty(Message.HDR_LARGE_BODY_SIZE, deflaterReader.getTotalSize());
                  msgI.getBodyBuffer().writeBytes(scratchBuffer, 0, bytesRead);
                  sendRegularMessage(msgI.getAddressSimpleString(), msgI, sendBlocking, credits, handler);
                  break;
               }
               if (!headerSent) {
                  sendInitialLargeMessageHeader(msgI, credits);
               }
               final long messageBodySize = deflaterReader != null ? deflaterReader.getTotalSize() : totalBytesRead;
               int creditsSent = sessionContext.sendLargeMessageChunk(msgI, messageBodySize, sendBlocking, true, trimmedBuffer(scratchBuffer, bytesRead), reconnectID, handler);
               credits.acquireCredits(creditsSent);
               break;
            }
            // !lastPacket
            if (!headerSent) {
               headerSent = true;
               sendInitialLargeMessageHeader(msgI, credits);
            }

            int creditsSent = sessionContext.sendLargeMessageChunk(msgI, 0, sendBlocking, false, trimmedBuffer(scratchBuffer, bytesRead), reconnectID, handler);
            credits.acquireCredits(creditsSent);
         }
      } catch (IOException e) {
         throw ActiveMQClientMessageBundle.BUNDLE.errorClosingLargeMessage(e);
      }
   }

   /**
    * This is trimming {@code buffer} to the expected size or reusing it if not needed.
    */
   private static byte[] trimmedBuffer(byte[] buffer, int expectedSize) {
      if (buffer.length == expectedSize) {
         return buffer;
      }
      byte[] trimmedBuffer = new byte[expectedSize];
      System.arraycopy(buffer, 0, trimmedBuffer, 0, expectedSize);
      return trimmedBuffer;
   }

   private static boolean isLargeMessageLastPacket(int readResult) {
      return readResult <= 0;
   }

   private static int bytesRead(int readResult) {
      return readResult > 0 ? readResult : -readResult;
   }

   /**
    * Use {@link #isLargeMessageLastPacket(int)} and {@link #bytesRead(int)} to decode the result of this method.
    */
   private static int readLargeMessageChunk(InputStream inputStream,
                                            byte[] readBuffer,
                                            int chunkLimit) throws ActiveMQLargeMessageException {
      assert chunkLimit > 0;
      int bytesRead = 0;
      do {
         final int remaining = chunkLimit - bytesRead;
         final int read;
         try {
            read = inputStream.read(readBuffer, bytesRead, remaining);
         } catch (IOException e) {
            throw ActiveMQClientMessageBundle.BUNDLE.errorReadingBody(e);
         }
         if (read == -1) {
            // bytesRead can be 0 if the stream return -1 after 0-length reads
            return -bytesRead;
         }
         bytesRead += read;
      }
      while (bytesRead < chunkLimit);
      assert bytesRead == chunkLimit;
      return bytesRead;
   }

}
