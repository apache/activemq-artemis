/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.critical;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class CriticalAnalyzerTest {

   @Rule
   public ThreadLeakCheckRule rule = new ThreadLeakCheckRule();

   private CriticalAnalyzer analyzer;

   @After
   public void tearDown() throws Exception {
      if (analyzer != null) {
         analyzer.stop();
      }
   }

   @Test
   public void testAction() throws Exception {
      analyzer = new CriticalAnalyzerImpl().setTimeout(100, TimeUnit.MILLISECONDS).setCheckTime(50, TimeUnit.MILLISECONDS);
      analyzer.add(new CriticalComponent() {
         @Override
         public boolean checkExpiration(long timeout, boolean reset) {
            return true;
         }
      });

      CountDownLatch latch = new CountDownLatch(1);

      analyzer.start();

      analyzer.addAction((CriticalComponent comp) -> {
         latch.countDown();
      });

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      analyzer.stop();
   }

   @Test
   public void testActionOnImpl() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      analyzer = new CriticalAnalyzerImpl().setTimeout(10, TimeUnit.MILLISECONDS).setCheckTime(5, TimeUnit.MILLISECONDS);

      analyzer.addAction((CriticalComponent comp) -> {
         latch.countDown();
      });

      CriticalComponent component = new CriticalComponentImpl(analyzer, 2);
      analyzer.add(component);

      component.enterCritical(0);
      component.leaveCritical(0);
      component.enterCritical(1);


      analyzer.start();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      analyzer.stop();
   }

   @Test
   public void testEnterNoLeaveNoExpire() throws Exception {
      analyzer = new CriticalAnalyzerImpl().setTimeout(10, TimeUnit.MILLISECONDS).setCheckTime(5, TimeUnit.MILLISECONDS);
      CriticalComponent component = new CriticalComponentImpl(analyzer, 2);
      component.enterCritical(0);
      Assert.assertFalse(component.checkExpiration(TimeUnit.MINUTES.toNanos(1), false));
      analyzer.stop();

   }

   @Test
   public void testEnterNoLeaveExpire() throws Exception {
      analyzer = new CriticalAnalyzerImpl().setTimeout(10, TimeUnit.MILLISECONDS).setCheckTime(5, TimeUnit.MILLISECONDS);
      CriticalComponent component = new CriticalComponentImpl(analyzer, 2);
      component.enterCritical(0);
      Thread.sleep(50);
      Assert.assertTrue(component.checkExpiration(0, false));
      analyzer.stop();

   }

   @Test
   public void testNegative() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      analyzer = new CriticalAnalyzerImpl().setTimeout(10, TimeUnit.MILLISECONDS).setCheckTime(5, TimeUnit.MILLISECONDS);

      analyzer.addAction((CriticalComponent comp) -> {
         latch.countDown();
      });

      CriticalComponent component = new CriticalComponentImpl(analyzer, 1);
      analyzer.add(component);

      component.enterCritical(0);
      component.leaveCritical(0);

      analyzer.start();

      Assert.assertFalse(latch.await(100, TimeUnit.MILLISECONDS));

      analyzer.stop();
   }

   @Test
   public void testPositive() throws Exception {
      ReusableLatch latch = new ReusableLatch(1);

      analyzer = new CriticalAnalyzerImpl().setTimeout(10, TimeUnit.MILLISECONDS).setCheckTime(5, TimeUnit.MILLISECONDS);

      analyzer.addAction((CriticalComponent comp) -> {
         latch.countDown();
      });

      CriticalComponent component = new CriticalComponentImpl(analyzer, 1);
      analyzer.add(component);

      component.enterCritical(0);
      Thread.sleep(50);

      analyzer.start();

      Assert.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));

      component.leaveCritical(0);

      latch.setCount(1);

      Assert.assertFalse(latch.await(100, TimeUnit.MILLISECONDS));

      analyzer.stop();
   }
}