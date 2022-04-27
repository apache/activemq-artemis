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

import javax.jms.JMSException;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OpenTracingPluginTest {

   private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
   @InjectMocks
   private OpenTracingPlugin plugin;

   @Mock
   private SpanBuilder spanBuilder;

   @Mock
   private Transaction tx;

   @Mock
   private Message message;

   @Mock
   private Span span;

   @Test
   public void assertBeforeMessage() throws JMSException, ActiveMQException {
      Tracer tracer = mock(Tracer.class);
      ServerSession session = mock(ServerSession.class);

      Mockito.doReturn(spanBuilder).when(tracer).spanBuilder(anyString());
      Mockito.doReturn(spanBuilder).when(spanBuilder).setAttribute(anyString(), anyString());
      Mockito.doReturn(spanBuilder).when(spanBuilder).setSpanKind(any(SpanKind.class));
      Mockito.doReturn(span).when(spanBuilder).startSpan();

      plugin.setTracer(tracer);
      plugin.beforeSend(session, tx, message, true, true);

      verify(tracer, atLeastOnce()).spanBuilder(anyString());
      verify(spanBuilder, atLeastOnce()).setAttribute(anyString(), anyString());
      verify(spanBuilder, atLeastOnce()).setSpanKind(any(SpanKind.class));
      verify(spanBuilder, atLeastOnce()).startSpan();
      verify(message, atLeastOnce()).setUserContext(any(Object.class), any(Object.class));
   }

   @Test
   public void assetAfterSend() throws ActiveMQException {
      when(message.getUserContext(Span.class)).thenReturn(span);

      plugin.afterSend(tx, message, true, true, RoutingStatus.OK);
      verify(span, atLeastOnce()).addEvent(anyString());
   }

   @Test
   public void assertOnSendException() throws ActiveMQException {
      ServerSession session = mock(ServerSession.class);
      Exception exception = mock(Exception.class);

      when(message.getUserContext(Span.class)).thenReturn(span);
      when(span.setStatus(any(StatusCode.class))).thenReturn(span);
      when(span.recordException(any(Exception.class))).thenReturn(span);

      plugin.onSendException(session, tx, message, true, true, exception);

      verify(span, atLeastOnce()).setStatus(any(StatusCode.class));
      verify(span, atLeastOnce()).setStatus(any(StatusCode.class));
      verify(span, atLeastOnce()).recordException(any(Exception.class));
   }

   @Test
   public void assertAfterDeliver() throws ActiveMQException {
      ServerConsumer consumer = mock(ServerConsumer.class);
      MessageReference reference = mock(MessageReference.class);
      Message mockMessage = mock(Message.class);

      when(reference.getMessage()).thenReturn(mockMessage);
      when(mockMessage.getUserContext(Span.class)).thenReturn(span);

      plugin.afterDeliver(consumer, reference);

      verify(span, atLeastOnce()).addEvent(anyString());
      verify(span, atLeastOnce()).end();
   }

   @After
   public void cleanUp() {
      exporter.reset();
      GlobalOpenTelemetry.resetForTest();
      validateMockitoUsage();
   }
}
