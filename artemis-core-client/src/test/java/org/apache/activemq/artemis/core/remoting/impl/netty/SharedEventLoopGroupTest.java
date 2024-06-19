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
package org.apache.activemq.artemis.core.remoting.impl.netty;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.Test;

public class SharedEventLoopGroupTest {
   @Test
   public void testSharedEventLoopGroupCreateOnShutdown() throws InterruptedException {
      final CustomNioEventLoopGroup customNioEventLoopGroup = new CustomNioEventLoopGroup();
      final CyclicBarrier barrier = new CyclicBarrier(2);

      AtomicReference<SharedEventLoopGroup> sharedEventLoopGroup1 = new AtomicReference<>();
      Thread t1 = new Thread(() -> {
         sharedEventLoopGroup1.set(SharedEventLoopGroup.getInstance(threadFactory -> customNioEventLoopGroup));
         customNioEventLoopGroup.setCyclicBarrier(barrier);
         sharedEventLoopGroup1.get().shutdownGracefully();
         customNioEventLoopGroup.setCyclicBarrier(null);
      });
      t1.start();

      AtomicReference<SharedEventLoopGroup> sharedEventLoopGroup2 = new AtomicReference<>();
      Thread t2 = new Thread(() -> {
         try {
            barrier.await();
            sharedEventLoopGroup2.set(SharedEventLoopGroup.getInstance(threadFactory -> new NioEventLoopGroup(2, threadFactory)));
         } catch (InterruptedException e) {
            e.printStackTrace();
         } catch (BrokenBarrierException e) {
            e.printStackTrace();
         }
      });
      t2.start();

      t1.join();
      t2.join();
      assertTrue(sharedEventLoopGroup1.get() == sharedEventLoopGroup2.get());

      Thread.sleep(11000);
      assertFalse(sharedEventLoopGroup2.get().isShuttingDown());
   }

   private static class CustomNioEventLoopGroup extends NioEventLoopGroup {
      private CyclicBarrier barrier;

      public void setCyclicBarrier(CyclicBarrier barrier) {
         this.barrier = barrier;
      }

      // Here we wait 1sec to imitate the race condition referred by ARTEMIS-2257
      @Override
      public EventLoop next() {
         if (barrier != null) {
            try {
               // Wait for thread t2 arriving
               barrier.await();
               // Wait until thread t2 calling SharedEventLoopGroup.getInstance()
               Thread.sleep(500);
            } catch (InterruptedException e) {
               e.printStackTrace();
            } catch (BrokenBarrierException e) {
               e.printStackTrace();
            }
         }
         return super.next();
      }
   }
}