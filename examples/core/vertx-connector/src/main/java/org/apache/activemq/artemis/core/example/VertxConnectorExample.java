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
package org.apache.activemq.artemis.core.example;

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;
import org.vertx.java.spi.cluster.impl.hazelcast.HazelcastClusterManagerFactory;

/**
 * A simple example of using Vert.x connector service.
 */
public class VertxConnectorExample
{
   public static final String INCOMING = "incoming.vertx.address";
   public static final String OUTGOING = "outgoing.vertx.address";
   public static final String MSG = "Welcome to Vertx world!";

   public final static CountDownLatch latch = new CountDownLatch(1);
   public final static AtomicBoolean result = new AtomicBoolean(false);

   private static final String HOST = "127.0.0.1";
   private static final int PORT = 0;

   public static void main(final String[] args) throws Exception
   {
      System.setProperty("vertx.clusterManagerFactory",
               HazelcastClusterManagerFactory.class.getName());
      PlatformManager platformManager = null;

      try
      {
         // Step 1 Create a Vert.x PlatformManager
         platformManager = PlatformLocator.factory.createPlatformManager(PORT, HOST);

         final CountDownLatch latch0 = new CountDownLatch(1);

         // Step 2 Deploy a Verticle to receive message
         String verticle = "org.apache.activemq.artemis.core.example.ExampleVerticle";
         platformManager.deployVerticle(verticle, null, new URL[0], 1, null,
                  new Handler<AsyncResult<String>>(){

                     @Override
                     public void handle(AsyncResult<String> result)
                     {
                        if (!result.succeeded())
                        {
                           throw new RuntimeException("failed to deploy verticle", result.cause());
                        }
                        latch0.countDown();
                     }

         });

         latch0.await();

         // Step 3 Send a message to the incoming connector's address
         EventBus bus = platformManager.vertx().eventBus();
         bus.send(INCOMING, MSG);

         // Step 4 Waiting for the Verticle to process the message
         latch.await(10000, TimeUnit.MILLISECONDS);
      }
      finally
      {
         if(platformManager != null)
         {
            platformManager.undeployAll(null);
            platformManager.stop();
         }
         reportResultAndExit();
      }
   }

   private static void reportResultAndExit()
   {
      if (!result.get())
      {
         System.err.println();
         System.err.println("#####################");
         System.err.println("###    FAILURE!   ###");
         System.err.println("#####################");
         System.exit(1);
      }
      else
      {
         System.out.println();
         System.out.println("#####################");
         System.out.println("###    SUCCESS!   ###");
         System.out.println("#####################");
         System.exit(0);
      }
   }

}
