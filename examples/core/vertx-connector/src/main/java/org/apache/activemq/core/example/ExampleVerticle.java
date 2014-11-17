/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

public class ExampleVerticle extends Verticle
{
   @Override
   public void start()
   {
      EventBus eventBus = vertx.eventBus();

      final CountDownLatch latch0 = new CountDownLatch(1);

      // Register a handler on the outgoing connector's address
      eventBus.registerHandler(VertxConnectorExample.OUTGOING, 
               new Handler<Message<?>>() {
                  @Override
                  public void handle(Message<?> startMsg)
                  {
                     Object body = startMsg.body();
                     System.out.println("Verticle receives a message: " + body);
                     VertxConnectorExample.result.set(VertxConnectorExample.MSG.equals(body));
                     latch0.countDown();
                     //Tell the example to finish.
                     VertxConnectorExample.latch.countDown();
                  }
      });

      try
      {
         latch0.await(5000, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
      }
   }
}
