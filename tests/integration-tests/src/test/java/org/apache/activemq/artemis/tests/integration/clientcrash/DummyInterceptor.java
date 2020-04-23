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
package org.apache.activemq.artemis.tests.integration.clientcrash;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.jboss.logging.Logger;

public class DummyInterceptor implements Interceptor {

   private static final Logger log = Logger.getLogger(DummyInterceptor.class);

   boolean sendException = false;

   boolean changeMessage = false;

   AtomicInteger syncCounter = new AtomicInteger(0);

   public int getCounter() {
      return syncCounter.get();
   }

   public void clearCounter() {
      syncCounter.set(0);
   }

   @Override
   public boolean intercept(final Packet packet, final RemotingConnection conn) throws ActiveMQException {
      log.debug("DummyFilter packet = " + packet.getClass().getName());
      syncCounter.addAndGet(1);
      if (sendException) {
         throw new ActiveMQInternalErrorException();
      }
      if (changeMessage) {
         if (packet instanceof SessionReceiveMessage) {
            SessionReceiveMessage deliver = (SessionReceiveMessage) packet;
            log.debug("msg = " + deliver.getMessage().getClass().getName());
            deliver.getMessage().putStringProperty(new SimpleString("DummyInterceptor"), new SimpleString("was here"));
         }
      }
      return true;
   }

}
