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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.activemq.artemis.core.message.LargeBodyReader;
import org.apache.activemq.artemis.core.persistence.impl.journal.LargeServerMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Tests for basics of core large message writer
 */
public class AMQPTunneledCoreLargeMessageWriterTest {

   private static final byte DATA_DESCRIPTOR = 0x75;

   @Mock
   ProtonServerSenderContext serverSender;

   @Mock
   Sender protonSender;

   @Mock
   Session protonSession;

   @Mock
   Connection protonConnection;

   @Mock
   TransportImpl protonTransport;

   @Mock
   Delivery protonDelivery;

   @Mock
   MessageReference reference;

   @Mock
   LargeServerMessageImpl message;

   @Mock
   LargeBodyReader bodyReader;

   @Mock
   AMQPConnectionContext connectionContext;

   @Mock
   AMQPSessionContext sessionContext;

   @Mock
   AMQPSessionCallback sessionSPI;

   @Spy
   NullStorageManager nullStoreManager = new NullStorageManager();

   @Captor
   ArgumentCaptor<ReadableBuffer> tunneledCaptor;

   @BeforeEach
   public void setUp() {
      MockitoAnnotations.openMocks(this);

      when(serverSender.getSessionContext()).thenReturn(sessionContext);
      when(serverSender.getSender()).thenReturn(protonSender);
      when(serverSender.createDelivery(any(), anyInt())).thenReturn(protonDelivery);

      when(protonSender.getSession()).thenReturn(protonSession);
      when(protonSession.getConnection()).thenReturn(protonConnection);
      when(protonConnection.getTransport()).thenReturn(protonTransport);
      when(protonTransport.getOutboundFrameSizeLimit()).thenReturn(65535);

      when(reference.getMessage()).thenReturn(message);
      when(message.isLargeMessage()).thenReturn(true);
      when(message.getLargeBodyReader()).thenReturn(bodyReader);
      when(sessionContext.getSessionSPI()).thenReturn(sessionSPI);
      when(sessionContext.getAMQPConnectionContext()).thenReturn(connectionContext);
      when(connectionContext.flowControl(any())).thenReturn(true);
      when(sessionSPI.getStorageManager()).thenReturn(nullStoreManager);
   }

   @Test
   public void testWriterThrowsIllegalStateIfNotOpenedWhenWriteCalled() throws Exception {
      AMQPTunneledCoreLargeMessageWriter writer = new AMQPTunneledCoreLargeMessageWriter(serverSender);

      try {
         writer.writeBytes(reference);
         fail("Should throw as the writer was not opened.");
      } catch (IllegalStateException e) {
         // Expected
      }
   }

   @Test
   public void testNoWritesWhenProtonSenderIsLocallyClosed() throws Exception {
      AMQPTunneledCoreLargeMessageWriter writer = new AMQPTunneledCoreLargeMessageWriter(serverSender);

      when(protonSender.getLocalState()).thenReturn(EndpointState.CLOSED);

      writer.open(Mockito.mock(MessageReference.class));

      try {
         writer.writeBytes(reference);
      } catch (IllegalStateException e) {
         fail("Should not throw as the delivery is partial so no data should be read.");
      }

      verify(protonSender).getLocalState();

      verifyNoInteractions(reference);
      verifyNoInteractions(protonDelivery);
   }

   @Test
   public void testMessageEncodingWrittenToDeliveryWithoutAnnotations() throws Exception {
      doTestMessageEncodingWrittenToDeliveryWithAnnotations(false);
   }

   @Test
   public void testMessageEncodingWrittenToDeliveryWithAnnotations() throws Exception {
      doTestMessageEncodingWrittenToDeliveryWithAnnotations(true);
   }

   private void doTestMessageEncodingWrittenToDeliveryWithAnnotations(boolean deliveryAnnotations) throws Exception {
      AMQPTunneledCoreLargeMessageWriter writer = new AMQPTunneledCoreLargeMessageWriter(serverSender);

      writer.open(Mockito.mock(MessageReference.class));

      final ByteBuf expectedEncoding = Unpooled.buffer();

      final byte[] headersBytes = new byte[4];

      headersBytes[0] = 4;
      headersBytes[1] = 5;
      headersBytes[2] = 6;
      headersBytes[3] = 7;

      final byte[] payloadBytes = new byte[4];

      payloadBytes[0] = 1;
      payloadBytes[1] = 2;
      payloadBytes[2] = 3;
      payloadBytes[3] = 4;

      if (deliveryAnnotations) {
         final DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());

         annotations.getValue().put(Symbol.valueOf("a"), "a");
         annotations.getValue().put(Symbol.valueOf("b"), "b");
         annotations.getValue().put(Symbol.valueOf("c"), "c");

         writeDeliveryAnnotations(expectedEncoding, annotations);

         when(reference.getProtocolData(any())).thenReturn(annotations);
      }

      writeDataSection(expectedEncoding, headersBytes);
      writeDataSection(expectedEncoding, payloadBytes);

      when(protonSender.getLocalState()).thenReturn(EndpointState.ACTIVE);
      when(protonDelivery.isPartial()).thenReturn(true);
      when(message.getHeadersAndPropertiesEncodeSize()).thenReturn(headersBytes.length);

      // Provides the simulated encoded core headers and properties
      doAnswer(invocation -> {
         final ByteBuf buffer = invocation.getArgument(0);

         buffer.writeBytes(headersBytes);

         return null;
      }).when(message).encodeHeadersAndProperties(any(ByteBuf.class));

      when(bodyReader.getSize()).thenReturn((long) payloadBytes.length);

      final ByteBuf encodedByteBuf = Unpooled.buffer();
      final NettyWritable encodedBytes = new NettyWritable(encodedByteBuf);

      // Answer back with the amount of writable bytes
      doAnswer(invocation -> {
         final ByteBuffer buffer = invocation.getArgument(0);

         buffer.put(payloadBytes);

         return payloadBytes.length;
      }).when(bodyReader).readInto(any());

      // Capture the write for comparison, this avoid issues with released netty buffers
      doAnswer(invocation -> {
         final ReadableBuffer buffer = invocation.getArgument(0);

         encodedBytes.put(buffer);

         return null;
      }).when(protonSender).send(any());

      try {
         writer.writeBytes(reference);
      } catch (IllegalStateException e) {
         fail("Should not throw as the delivery is completed so no data should be written.");
      }

      verify(message).usageUp();
      verify(message).getLargeBodyReader();
      verify(message).getHeadersAndPropertiesEncodeSize();
      verify(message).encodeHeadersAndProperties(any(ByteBuf.class));
      verify(reference).getMessage();
      verify(reference).getProtocolData(any());
      verify(protonSender).getSession();
      verify(protonDelivery).getTag();
      verify(protonSender, atLeastOnce()).getLocalState();
      verify(protonSender, atLeastOnce()).send(any(ReadableBuffer.class));

      assertTrue(encodedByteBuf.isReadable());
      assertEquals(expectedEncoding.readableBytes(), encodedByteBuf.readableBytes());
      assertEquals(expectedEncoding, encodedByteBuf);

      verifyNoMoreInteractions(message);
      verifyNoMoreInteractions(reference);
      verifyNoMoreInteractions(protonDelivery);
   }

   @Test
   public void testLargeMessageUsageLoweredOnCloseWhenWriteNotCompleted() throws Exception {
      AMQPTunneledCoreLargeMessageWriter writer = new AMQPTunneledCoreLargeMessageWriter(serverSender);

      writer.open(Mockito.mock(MessageReference.class));

      when(protonSender.getLocalState()).thenReturn(EndpointState.ACTIVE);
      when(protonDelivery.isPartial()).thenReturn(true);

      // The writer will wait for flow control to resume it
      when(connectionContext.flowControl(any())).thenReturn(false);

      try {
         writer.writeBytes(reference);
      } catch (IllegalStateException e) {
         fail("Should not throw as the delivery is completed so no data should be written.");
      }

      try {
         writer.close();
      } catch (IllegalStateException e) {
         fail("Should not throw as the close when write wasn't completed.");
      }

      verify(message).usageUp();
      verify(message).usageDown();
      verify(reference).getMessage();
      verify(reference).getProtocolData(any());
      verify(protonSender).getSession();
      verify(protonDelivery).getTag();
      verify(protonSender, atLeastOnce()).getLocalState();

      verifyNoMoreInteractions(reference);
      verifyNoMoreInteractions(protonDelivery);
   }

   private void writeDeliveryAnnotations(ByteBuf buffer, DeliveryAnnotations annotations) {
      final EncoderImpl encoder = TLSEncode.getEncoder();

      try {
         encoder.setByteBuffer(new NettyWritable(buffer));
         encoder.writeObject(annotations);
      } finally {
         encoder.setByteBuffer((WritableBuffer) null);
      }
   }

   private void writeDataSection(ByteBuf buffer, byte[] payload) {
      buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
      buffer.writeByte(EncodingCodes.SMALLULONG);
      buffer.writeByte(DATA_DESCRIPTOR);
      buffer.writeByte(EncodingCodes.VBIN32);
      buffer.writeInt(payload.length);
      buffer.writeBytes(payload);
   }
}
