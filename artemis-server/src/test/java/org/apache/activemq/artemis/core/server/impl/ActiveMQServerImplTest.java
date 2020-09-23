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
package org.apache.activemq.artemis.core.server.impl;

import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class ActiveMQServerImplTest extends ActiveMQTestBase {

   @Test
   public void testAddingAndStartingExternalComponent() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      EmbeddedServerTest.FakeExternalComponent component = new EmbeddedServerTest.FakeExternalComponent();
      server.addExternalComponent(component, true);
      Assert.assertTrue(component.isStarted());
      Assert.assertThat(server.getExternalComponents(), hasItem(component));
   }

   @Test
   public void testAddingWithoutStartingExternalComponent() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      EmbeddedServerTest.FakeExternalComponent component = new EmbeddedServerTest.FakeExternalComponent();
      server.addExternalComponent(component, false);
      Assert.assertFalse(component.isStarted());
      Assert.assertThat(server.getExternalComponents(), hasItem(component));
   }

   @Test
   public void testCannotAddExternalComponentsIfNotStarting() throws Exception {
      ActiveMQServer server = createServer(false);
      EmbeddedServerTest.FakeExternalComponent component = new EmbeddedServerTest.FakeExternalComponent();
      try {
         server.addExternalComponent(component, false);
         Assert.fail();
      } catch (IllegalStateException ex) {
         Assert.assertFalse(component.isStarted());
         Assert.assertThat(server.getExternalComponents(), empty());
      }
   }

   @Test
   public void testCannotAddExternalComponentsIfStopped() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      server.stop();
      EmbeddedServerTest.FakeExternalComponent component = new EmbeddedServerTest.FakeExternalComponent();
      try {
         server.addExternalComponent(component, false);
         Assert.fail();
      } catch (IllegalStateException ex) {
         Assert.assertFalse(component.isStarted());
         Assert.assertThat(server.getExternalComponents(), empty());
      }
   }

   @Test
   public void testScheduledPoolGC() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();

      Runnable scheduledRunnable = new Runnable() {
         @Override
         public void run() {
            Assert.fail();
         }
      };
      WeakReference<Runnable> scheduledRunnableRef = new WeakReference<>(scheduledRunnable);

      ScheduledExecutorService scheduledPool = server.getScheduledPool();
      ScheduledFuture scheduledFuture = scheduledPool.schedule(scheduledRunnable, 5000, TimeUnit.MILLISECONDS);

      Assert.assertFalse(scheduledFuture.isCancelled());
      Assert.assertTrue(scheduledFuture.cancel(true));
      Assert.assertTrue(scheduledFuture.isCancelled());

      Assert.assertNotEquals(null, scheduledRunnableRef.get());

      scheduledRunnable = null;

      forceGC();

      Assert.assertEquals(null, scheduledRunnableRef.get());

      server.stop();
   }

}
