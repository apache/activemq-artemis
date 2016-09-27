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

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.jboss.logging.Logger;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ExtCapability;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AMQPConnectionContext extends ProtonInitializable  {

   private static final Logger log = Logger.getLogger(AMQPConnectionContext.class);

   public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
   public static final String AMQP_CONTAINER_ID = "amqp-container-id";

   protected final ProtonHandler handler;

   protected AMQPConnectionCallback connectionCallback;
   private final String containerId;
   private final Map<Symbol, Object> connectionProperties = new HashMap<>();
   private final ScheduledExecutorService scheduledPool;

   private final Map<Session, AMQPSessionContext> sessions = new ConcurrentHashMap<>();

   protected LocalListener listener = new LocalListener();



   public AMQPConnectionContext(AMQPConnectionCallback connectionSP,
                                String containerId,
                                int idleTimeout,
                                int maxFrameSize,
                                int channelMax,
                                Executor dispatchExecutor,
                                ScheduledExecutorService scheduledPool) {
      this.connectionCallback = connectionSP;
      this.containerId = (containerId != null) ? containerId : UUID.randomUUID().toString();

      connectionProperties.put(AmqpSupport.PRODUCT, "apache-activemq-artemis");
      connectionProperties.put(AmqpSupport.VERSION, VersionLoader.getVersion().getFullVersion());

      this.scheduledPool = scheduledPool;
      connectionCallback.setConnection(this);
      this.handler = new ProtonHandler(dispatchExecutor);
      Transport transport = handler.getTransport();
      transport.setEmitFlowEventOnSend(false);
      if (idleTimeout > 0) {
         transport.setIdleTimeout(idleTimeout);
      }
      transport.setChannelMax(channelMax);
      transport.setMaxFrameSize(maxFrameSize);
      handler.addEventHandler(listener);
   }

   protected AMQPSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionCallback sessionSPI = connectionCallback.createSessionCallback(this);
      AMQPSessionContext protonSession = new AMQPSessionContext(sessionSPI, this, realSession);

      return protonSession;
   }

   public SASLResult getSASLResult() {
      return handler.getSASLResult();
   }

   public void inputBuffer(ByteBuf buffer) {
      if (log.isTraceEnabled()) {
         ByteUtil.debugFrame(log, "Buffer Received ", buffer);
      }

      handler.inputBuffer(buffer);
   }

   public void destroy() {
      connectionCallback.close();
   }

   public boolean isSyncOnFlush() {
      return false;
   }

   public Object getLock() {
      return handler.getLock();
   }

   public int capacity() {
      return handler.capacity();
   }

   public void outputDone(int bytes) {
      handler.outputDone(bytes);
   }

   public void flush() {
      handler.flush();
   }

   public void close() {
      handler.close();
   }


   protected AMQPSessionContext getSessionExtension(Session realSession) throws ActiveMQAMQPException {
      AMQPSessionContext sessionExtension = sessions.get(realSession);
      if (sessionExtension == null) {
         // how this is possible? Log a warn here
         sessionExtension = newSessionExtension(realSession);
         realSession.setContext(sessionExtension);
         sessions.put(realSession, sessionExtension);
      }
      return sessionExtension;
   }



   protected boolean validateConnection(Connection connection) {
      return connectionCallback.validateConnection(connection, handler.getSASLResult());
   }

   public boolean checkDataReceived() {
      return handler.checkDataReceived();
   }

   public long getCreationTime() {
      return handler.getCreationTime();
   }

   protected void flushBytes() {
      ByteBuf bytes;
      // handler.outputBuffer has the lock
      while ((bytes = handler.outputBuffer()) != null) {
         connectionCallback.onTransport(bytes, this);
      }
   }

   public String getRemoteContainer() {
      return handler.getConnection().getRemoteContainer();
   }

   public String getPubSubPrefix() {
      return null;
   }

   protected void initInternal() throws Exception {
   }

   protected void remoteLinkOpened(Link link) throws Exception {

      AMQPSessionContext protonSession = (AMQPSessionContext) getSessionExtension(link.getSession());

      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver) {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         }
         else {
            protonSession.addReceiver(receiver);
         }
      }
      else {
         Sender sender = (Sender) link;
         protonSession.addSender(sender);
         sender.offer(1);
      }
   }

   public Symbol[] getConnectionCapabilitiesOffered() {
      return ExtCapability.getCapabilities();
   }



   // This listener will perform a bunch of things here
   class LocalListener implements EventHandler {

      @Override
      public void onInit(Connection connection) throws Exception {

      }

      @Override
      public void onLocalOpen(Connection connection) throws Exception {

      }

      @Override
      public void onLocalClose(Connection connection) throws Exception {

      }

      @Override
      public void onFinal(Connection connection) throws Exception {

      }

      @Override
      public void onInit(Session session) throws Exception {

      }

      @Override
      public void onFinal(Session session) throws Exception {

      }

      @Override
      public void onInit(Link link) throws Exception {

      }

      @Override
      public void onLocalOpen(Link link) throws Exception {

      }

      @Override
      public void onLocalClose(Link link) throws Exception {

      }

      @Override
      public void onFinal(Link link) throws Exception {

      }

      @Override
      public void onAuthInit(ProtonHandler handler, Connection connection, boolean sasl) {
         if (sasl) {
            handler.createServerSASL(connectionCallback.getSASLMechnisms());
         }
         else {
            if (!connectionCallback.isSupportsAnonymous()) {
               connectionCallback.sendSASLSupported();
               connectionCallback.close();
               handler.close();
            }
         }
      }

      @Override
      public void onTransport(Transport transport) {
         flushBytes();
      }

      @Override
      public void onRemoteOpen(Connection connection) throws Exception {
         synchronized (getLock()) {
            try {
               initInternal();
            }
            catch (Exception e) {
               log.error("Error init connection", e);
            }
            if (!validateConnection(connection)) {
               connection.close();
            }
            else {
               connection.setContext(AMQPConnectionContext.this);
               connection.setContainer(containerId);
               connection.setProperties(connectionProperties);
               connection.setOfferedCapabilities(getConnectionCapabilitiesOffered());
               connection.open();
            }
         }
         initialise();

         /*
         * This can be null which is in effect an empty map, also we really dont need to check this for in bound connections
         * but its here in case we add support for outbound connections.
         * */
         if (connection.getRemoteProperties() == null || !connection.getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {
            long nextKeepAliveTime = handler.tick(true);
            flushBytes();
            if (nextKeepAliveTime > 0 && scheduledPool != null) {
               scheduledPool.schedule(new Runnable() {
                  @Override
                  public void run() {
                     long rescheduleAt = (handler.tick(false) - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
                     flushBytes();
                     if (rescheduleAt > 0) {
                        scheduledPool.schedule(this, rescheduleAt, TimeUnit.MILLISECONDS);
                     }
                  }
               }, (nextKeepAliveTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime())), TimeUnit.MILLISECONDS);
            }
         }
      }

      @Override
      public void onRemoteClose(Connection connection) {
         synchronized (getLock()) {
            connection.close();
            for (AMQPSessionContext protonSession : sessions.values()) {
               protonSession.close();
            }
            sessions.clear();
         }
         // We must force write the channel before we actually destroy the connection
         onTransport(handler.getTransport());
         destroy();
      }

      @Override
      public void onLocalOpen(Session session) throws Exception {
         getSessionExtension(session);
      }

      @Override
      public void onRemoteOpen(Session session) throws Exception {
         getSessionExtension(session).initialise();
         synchronized (getLock()) {
            session.open();
         }
      }

      @Override
      public void onLocalClose(Session session) throws Exception {
      }

      @Override
      public void onRemoteClose(Session session) throws Exception {
         synchronized (getLock()) {
            session.close();
         }

         AMQPSessionContext sessionContext = (AMQPSessionContext)session.getContext();
         if (sessionContext != null) {
            sessionContext.close();
            sessions.remove(session);
            session.setContext(null);
         }
      }

      @Override
      public void onRemoteOpen(Link link) throws Exception {
         remoteLinkOpened(link);
      }

      @Override
      public void onFlow(Link link) throws Exception {
         ((ProtonDeliveryHandler) link.getContext()).onFlow(link.getCredit(), link.getDrain());
      }

      @Override
      public void onRemoteClose(Link link) throws Exception {
         link.close();
         ProtonDeliveryHandler linkContext = (ProtonDeliveryHandler) link.getContext();
         if (linkContext != null) {
            linkContext.close(true);
         }
      }

      @Override
      public void onRemoteDetach(Link link) throws Exception {
         link.detach();
      }

      @Override
      public void onDetach(Link link) throws Exception {
         Object context = link.getContext();
         if (context instanceof ProtonServerSenderContext) {
            ProtonServerSenderContext senderContext = (ProtonServerSenderContext) context;
            senderContext.close(false);
         }
      }

      @Override
      public void onDelivery(Delivery delivery) throws Exception {
         ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
         if (handler != null) {
            handler.onMessage(delivery);
         }
         else {
            // TODO: logs

            System.err.println("Handler is null, can't delivery " + delivery);
         }
      }
   }


}
