/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.sasl.ProtonSaslAuthenticatorFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class MockServer {
   private ProtonServer server;

   public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler) throws ExecutionException, InterruptedException {
      this(vertx, new ProtonServerOptions(), null, connectionHandler);
   }

   public MockServer(Vertx vertx, ProtonSaslAuthenticatorFactory authFactory, Handler<ProtonConnection> connectionHandler) throws ExecutionException, InterruptedException {
      this(vertx, new ProtonServerOptions(), authFactory, connectionHandler);
   }

   public MockServer(Vertx vertx, ProtonServerOptions options, ProtonSaslAuthenticatorFactory authFactory, Handler<ProtonConnection> connectionHandler) throws ExecutionException, InterruptedException {
      Objects.requireNonNull(options, "options must not be null");

      server = ProtonServer.create(vertx, options);
      server.connectHandler(connectionHandler);
      if (authFactory != null) {
         server.saslAuthenticatorFactory(authFactory);
      }

      AtomicReference<Throwable> failure = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);

      // Passing port 0 to have the server choose port at bind.
      // Use actualPort() to discover port used.
      server.listen(0, res -> {
         if (!res.succeeded()) {
            failure.set(res.cause());
         }
         latch.countDown();
      });

      latch.await();

      if (failure.get() != null) {
         throw new ExecutionException(failure.get());
      }
   }

   public int actualPort() {
      return server.actualPort();
   }

   public void close() {
      server.close();
   }

   ProtonServer getProtonServer() {
      return server;
   }
}
