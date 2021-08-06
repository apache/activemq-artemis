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

package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.core.server.balancing.targets.MockTargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.MockTargetProbe;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

public abstract class PoolTestBase {
   public static final int MULTIPLE_TARGETS = 10;

   public static final int CHECK_PERIOD = 100;
   public static final int CHECK_TIMEOUT = 2 * CHECK_PERIOD;


   protected abstract Pool createPool(TargetFactory targetFactory, int targets);


   @Test
   public void testPoolWithNoTargets() throws Exception {
      testPoolTargets(0);
   }

   @Test
   public void testPoolWithSingleTarget() throws Exception {
      testPoolTargets(1);
   }

   @Test
   public void testPoolWithMultipleTargets() throws Exception {
      testPoolTargets(MULTIPLE_TARGETS);
   }

   @Test
   public void testPoolQuorumWithMultipleTargets() throws Exception {
      final int targets = MULTIPLE_TARGETS;
      final int quorumSize = 2;

      Assert.assertTrue(targets - quorumSize > 2);

      MockTargetFactory targetFactory = new MockTargetFactory().setConnectable(true).setReady(true);
      Pool pool = createPool(targetFactory, targets);

      pool.setQuorumSize(quorumSize);

      Assert.assertEquals(0, pool.getTargets().size());

      pool.start();

      Wait.assertEquals(targets, () -> pool.getTargets().size(), CHECK_TIMEOUT);

      targetFactory.getCreatedTargets().stream().limit(targets - quorumSize + 1)
         .forEach(mockTarget -> mockTarget.setReady(false));

      Wait.assertEquals(0, () -> pool.getTargets().size(), CHECK_TIMEOUT);

      targetFactory.getCreatedTargets().get(0).setReady(true);

      Wait.assertEquals(quorumSize, () -> pool.getTargets().size(), CHECK_TIMEOUT);

      pool.setQuorumSize(quorumSize + 1);

      Wait.assertEquals(0, () -> pool.getTargets().size(), CHECK_TIMEOUT);

      targetFactory.getCreatedTargets().get(1).setReady(true);

      Wait.assertEquals(quorumSize + 1, () -> pool.getTargets().size(), CHECK_TIMEOUT);
   }


   private void testPoolTargets(int targets) throws Exception {
      MockTargetFactory targetFactory = new MockTargetFactory();
      MockTargetProbe targetProbe = new MockTargetProbe("TEST", false);
      Pool pool = createPool(targetFactory, targets);

      pool.addTargetProbe(targetProbe);

      Assert.assertEquals(0, pool.getTargets().size());
      Assert.assertEquals(0, pool.getAllTargets().size());
      Assert.assertEquals(0, targetFactory.getCreatedTargets().size());
      targetFactory.getCreatedTargets().forEach(mockTarget -> {
         Assert.assertFalse(pool.isTargetReady(mockTarget));
         Assert.assertEquals(0, targetProbe.getTargetExecutions(mockTarget));
      });

      pool.start();

      try {
         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(targets, pool.getAllTargets().size());
         Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
         targetFactory.getCreatedTargets().forEach(mockTarget -> {
            Assert.assertFalse(pool.isTargetReady(mockTarget));
            Assert.assertEquals(0, targetProbe.getTargetExecutions(mockTarget));
         });

         if (targets > 0) {
            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

            Assert.assertEquals(0, pool.getTargets().size());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertFalse(pool.isTargetReady(mockTarget));
               Assert.assertEquals(0, targetProbe.getTargetExecutions(mockTarget));
            });

            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(true));

            Assert.assertEquals(0, pool.getTargets().size());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertFalse(pool.isTargetReady(mockTarget));
               Wait.assertTrue(() -> targetProbe.getTargetExecutions(mockTarget) > 0, CHECK_TIMEOUT);
            });

            targetProbe.setChecked(true);

            Wait.assertEquals(targets, () -> pool.getTargets().size(), CHECK_TIMEOUT);
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertTrue(pool.isTargetReady(mockTarget));
               Assert.assertTrue(targetProbe.getTargetExecutions(mockTarget) > 0);
            });

            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               mockTarget.setConnectable(false);
               try {
                  mockTarget.disconnect();
               } catch (Exception ignore) {
               }
            });

            Wait.assertEquals(0, () -> pool.getTargets().size(), CHECK_TIMEOUT);
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertFalse(pool.isTargetReady(mockTarget));
               Assert.assertTrue(targetProbe.getTargetExecutions(mockTarget) > 0);
            });

            targetProbe.clearTargetExecutions();

            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

            Wait.assertEquals(targets, () -> pool.getTargets().size(), CHECK_TIMEOUT);
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertTrue(pool.isTargetReady(mockTarget));
               Assert.assertTrue(targetProbe.getTargetExecutions(mockTarget) > 0);
            });

            targetProbe.clearTargetExecutions();

            targetProbe.setChecked(false);

            Wait.assertEquals(0, () -> pool.getTargets().size(), CHECK_TIMEOUT);
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               Assert.assertFalse(pool.isTargetReady(mockTarget));
               Assert.assertTrue(targetProbe.getTargetExecutions(mockTarget) > 0);
            });
         }
      } finally {
         pool.stop();
      }
   }
}
