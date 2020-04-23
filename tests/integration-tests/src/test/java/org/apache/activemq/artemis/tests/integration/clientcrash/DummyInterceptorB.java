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
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.jboss.logging.Logger;

public class DummyInterceptorB implements Interceptor {

   private static final Logger log = Logger.getLogger(DummyInterceptorB.class);

   static AtomicInteger syncCounter = new AtomicInteger(0);

   public static int getCounter() {
      return DummyInterceptorB.syncCounter.get();
   }

   public static void clearCounter() {
      DummyInterceptorB.syncCounter.set(0);
   }

   @Override
   public boolean intercept(final Packet packet, final RemotingConnection conn) throws ActiveMQException {
      DummyInterceptorB.syncCounter.addAndGet(1);
      log.debug("DummyFilter packet = " + packet);
      return true;
   }
}
