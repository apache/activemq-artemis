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
package org.proton.plug.context;

import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;

import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.junit.Test;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.SASLResult;
import org.proton.plug.ServerSASL;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.handler.EventHandler;

public class AbstractConnectionContextTest {

   @Test
   public void testListenerDoesntThrowNPEWhenClosingLinkWithNullContext() throws Exception {
      TestConnectionContext connectionContext = new TestConnectionContext(new TestConnectionCallback());
      EventHandler listener = connectionContext.getListener();

      Connection protonConnection = Connection.Factory.create();
      Session protonSession = protonConnection.session();
      Link link = protonSession.receiver("link");

      link.setContext(null);

      listener.onRemoteClose(link);
   }

   private class TestConnectionContext extends AbstractConnectionContext {

      private TestConnectionContext(AMQPConnectionCallback connectionCallback) {
         super(connectionCallback, Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory()), null);
      }

      @Override
      protected void remoteLinkOpened(Link link) throws Exception {

      }

      @Override
      protected AbstractProtonSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException {
         return null;
      }

      public EventHandler getListener() {
         return listener;
      }
   }

   private class TestConnectionCallback implements AMQPConnectionCallback {

      @Override
      public void close() {

      }

      @Override
      public void onTransport(ByteBuf bytes, AMQPConnectionContext connection) {

      }

      @Override
      public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
         return null;
      }

      @Override
      public void setConnection(AMQPConnectionContext connection) {

      }

      @Override
      public AMQPConnectionContext getConnection() {
         return null;
      }

      @Override
      public ServerSASL[] getSASLMechnisms() {
         return null;
      }

      @Override
      public boolean isSupportsAnonymous() {
         return true;
      }

      @Override
      public void sendSASLSupported() {

      }

      @Override
      public boolean validateConnection(Connection connection, SASLResult saslResult) {
         return true;
      }
   }
}
