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
package org.proton.plug.test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import java.lang.ref.WeakReference;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.jboss.logging.Logger;
import org.proton.plug.test.minimalserver.DumbServer;
import org.proton.plug.test.minimalserver.MinimalServer;

public class AbstractJMSTest {

   private static final Logger log = Logger.getLogger(AbstractJMSTest.class);

   protected final boolean useSASL;

   protected String address = "exampleQueue";
   protected MinimalServer server = new MinimalServer();

   public AbstractJMSTest(boolean useSASL) {
      this.useSASL = useSASL;
   }

   public void tearDown() throws Exception {
      server.stop();
      DumbServer.clear();
   }

   public static void forceGC() {
      System.out.println("#test forceGC");
      WeakReference<Object> dumbReference = new WeakReference<>(new Object());
      // A loop that will wait GC, using the minimalserver time as possible
      while (dumbReference.get() != null) {
         System.gc();
         try {
            Thread.sleep(100);
         }
         catch (InterruptedException e) {
         }
      }
      System.out.println("#test forceGC Done");
   }

   protected Connection createConnection() throws JMSException {
      final ConnectionFactory factory = createConnectionFactory();
      final Connection connection = factory.createConnection();
      connection.setExceptionListener(new ExceptionListener() {
         @Override
         public void onException(JMSException exception) {
            log.warn(exception.getMessage(), exception);
         }
      });
      connection.start();
      return connection;
   }

   protected ConnectionFactory createConnectionFactory() {
      if (useSASL) {
         return new JmsConnectionFactory("aaaaaaaa", "aaaaaaa", "amqp://localhost:5672");
      }
      else {
         return new JmsConnectionFactory( "amqp://localhost:5672");

      }
   }

   protected Queue createQueue(Session session) throws Exception {
      return session.createQueue(address);
   }

}
