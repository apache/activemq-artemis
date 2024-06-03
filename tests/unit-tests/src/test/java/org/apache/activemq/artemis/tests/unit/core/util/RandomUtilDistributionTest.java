/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.unit.core.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;

import org.apache.activemq.artemis.tests.extensions.SpawnedVMCheckExtension;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/**
 * This test will start many parallel VMs, to make sure each VM would generate a good distribution of random numbers
 */
public class RandomUtilDistributionTest {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @RegisterExtension
   public SpawnedVMCheckExtension check = new SpawnedVMCheckExtension();

   public static void main(String[] arg) {

      long start = Long.parseLong(arg[0]);

      try {
         Thread.sleep((start - System.currentTimeMillis()) / 2);
      } catch (Exception e) {
      }
      while (System.currentTimeMillis() < start) {
         Thread.yield();
      }
      int value;
      value = RandomUtil.randomInterval(0, 255);
      System.exit(value);
   }

   @Test
   public void testDistribution() throws Exception {
      int numberOfStarts = 50;
      int iterations = 1;

      int value = 0;
      for (int i = 0; i < iterations; i++) {

         int v = internalDistributionTest(numberOfStarts);

         value += v;
      }

      // I'm using an extra parenthesis here to avoid rounding problems.
      // Be careful removing it (make sure you know what you're doing in case you do so)
      int minimumExpected = (int) ((iterations * numberOfStarts) * 0.80);

      logger.debug("values = {}, minimum expected = {}", value, minimumExpected);
      assertTrue(value >= minimumExpected, "The Random distribution is pretty bad. Many tries have returned duplicated randoms. Number of different values=" + value + ", minimum expected = " + minimumExpected);
   }

   private int internalDistributionTest(int numberOfTries) throws Exception {
      long timeStart = System.currentTimeMillis() + 5000;
      Process[] process = new Process[numberOfTries];
      int[] value = new int[numberOfTries];
      try {
         for (int i = 0; i < numberOfTries; i++) {
            process[i] = SpawnedVMSupport.spawnVM(RandomUtilDistributionTest.class.getName(), true, "" + timeStart);
         }

         HashSet<Integer> valueSet = new HashSet<>();

         for (int i = 0; i < numberOfTries; i++) {
            value[i] = process[i].waitFor();
            assertTrue(value[i] >= 0);
            valueSet.add(process[i].exitValue());
         }

         logger.debug("Generated {} randoms out of {} tries", valueSet.size(), numberOfTries);

         return valueSet.size();

      } finally {
         for (Process p : process) {
            if (p != null) {
               p.destroy();
            }
         }
      }
   }

}
