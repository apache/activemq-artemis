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

import java.io.InputStream;
import java.util.Properties;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;

public class OpenTracingPlugin implements ActiveMQServerPlugin {

   private static final String OPERATION_NAME = "ArtemisMessageDelivery";
   private static OpenTelemetrySdk sdk = initOpenTracing();
   private static Tracer tracer = GlobalOpenTelemetry.getTracer(OpenTracingPlugin.class.getName());

   public static OpenTelemetrySdk initOpenTracing() {
      try {
         InputStream input = OpenTracingPlugin.class.getClassLoader().getResourceAsStream("tracing.properties");
         if (input == null) {
            throw new NullPointerException("Unable to find tracing.properties file");
         }
         Properties prop = new Properties(System.getProperties());
         prop.load(input);
         System.setProperties(prop);

         sdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();

      } catch (Throwable t) {
         t.printStackTrace();
      }
      return sdk;
   }

   @Override
   public void beforeSend(ServerSession session,
                          Transaction tx,
                          Message message,
                          boolean direct,
                          boolean noAutoCreateQueue) throws ActiveMQException {
      SpanBuilder spanBuilder = getTracer().spanBuilder(OPERATION_NAME).setAttribute("message", message.toString()).setSpanKind(SpanKind.SERVER);
      Span span = spanBuilder.startSpan();
      message.setUserContext(Span.class, span);
   }

   @Override
   public void afterSend(Transaction tx,
                         Message message,
                         boolean direct,
                         boolean noAutoCreateQueue,
                         RoutingStatus result) throws ActiveMQException {
      Span span = getSpan(message);
      span.addEvent("send " + result.name());
   }

   @Override
   public void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      Span span = (Span) reference.getMessage().getUserContext(Span.class);
      span.addEvent("deliver " + consumer.getSessionName());
      span.end();
   }

   @Override
   public void onSendException(ServerSession session,
                               Transaction tx,
                               Message message,
                               boolean direct,
                               boolean noAutoCreateQueue,
                               Exception e) throws ActiveMQException {
      getSpan(message).setStatus(StatusCode.ERROR).recordException(e);
   }

   public Tracer getTracer() {
      return tracer;
   }

   public void setTracer(Tracer myTracer) {
      tracer = myTracer;
   }

   private Span getSpan(Message message) {
      Span span = (Span) message.getUserContext(Span.class);
      return span;
   }
}