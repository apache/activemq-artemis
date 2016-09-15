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
package org.proton.plug.test.invm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.qpid.proton.engine.Connection;
import org.jboss.logging.Logger;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.SASLResult;
import org.proton.plug.ServerSASL;
import org.proton.plug.context.server.ProtonServerConnectionContext;
import org.proton.plug.sasl.AnonymousServerSASL;
import org.proton.plug.sasl.ServerSASLPlain;
import org.proton.plug.test.minimalserver.MinimalSessionSPI;
import org.proton.plug.util.ByteUtil;

public class ProtonINVMSPI implements AMQPConnectionCallback {

   private static final Logger log = Logger.getLogger(ProtonINVMSPI.class);


   AMQPConnectionContext returningConnection;

   ProtonServerConnectionContext serverConnection = new ProtonServerConnectionContext(new ReturnSPI(), Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory()), null);

   final ExecutorService mainExecutor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());

   final ExecutorService returningExecutor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());

   public ProtonINVMSPI() {
      mainExecutor.execute(new Runnable() {
         @Override
         public void run() {
            Thread.currentThread().setName("MainExecutor-INVM");
         }
      });
      returningExecutor.execute(new Runnable() {
         @Override
         public void run() {
            Thread.currentThread().setName("ReturningExecutor-INVM");
         }
      });
   }

   @Override
   public void close() {
      mainExecutor.shutdown();
   }

   @Override
   public ServerSASL[] getSASLMechnisms() {
      return new ServerSASL[]{new AnonymousServerSASL(), new ServerSASLPlain()};
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

   @Override
   public void onTransport(final ByteBuf bytes, final AMQPConnectionContext connection) {
      if (log.isTraceEnabled()) {
         ByteUtil.debugFrame(log, "InVM->", bytes);
      }
      final int size = bytes.writerIndex();

      bytes.retain();
      mainExecutor.execute(new Runnable() {
         @Override
         public void run() {
            try {
               if (log.isTraceEnabled()) {
                  ByteUtil.debugFrame(log, "InVMDone->", bytes);
               }
               serverConnection.inputBuffer(bytes);
               try {
                  connection.outputDone(size);
               }
               catch (Exception e) {
                  log.warn(e.getMessage(), e);
               }
            }
            finally {
               bytes.release();
            }
         }
      });
   }

   @Override
   public void setConnection(AMQPConnectionContext connection) {
      returningConnection = connection;
   }

   @Override
   public AMQPConnectionContext getConnection() {
      return returningConnection;
   }

   @Override
   public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
      return null;
   }

   class ReturnSPI implements AMQPConnectionCallback {

      @Override
      public void close() {

      }

      @Override
      public ServerSASL[] getSASLMechnisms() {
         return new ServerSASL[]{new AnonymousServerSASL(), new ServerSASLPlain()};
      }

      @Override
      public boolean isSupportsAnonymous() {
         return false;
      }

      @Override
      public void sendSASLSupported() {

      }

      @Override
      public boolean validateConnection(Connection connection, SASLResult saslResult) {
         return true;
      }

      @Override
      public void onTransport(final ByteBuf bytes, final AMQPConnectionContext connection) {

         final int size = bytes.writerIndex();
         ByteUtil.debugFrame(log, "InVM<-", bytes);

         bytes.retain();
         returningExecutor.execute(new Runnable() {
            @Override
            public void run() {
               try {

                  ByteUtil.debugFrame(log, "InVM done<-", bytes);

                  returningConnection.inputBuffer(bytes);
                  try {
                     connection.outputDone(size);
                  }
                  catch (Exception e) {
                     log.warn(e.getMessage(), e);
                  }

               }
               finally {
                  bytes.release();
               }
            }
         });
      }

      @Override
      public AMQPSessionCallback createSessionCallback(AMQPConnectionContext connection) {
         return new MinimalSessionSPI();
      }

      @Override
      public void setConnection(AMQPConnectionContext connection) {

      }

      @Override
      public AMQPConnectionContext getConnection() {
         return null;
      }
   }
}
