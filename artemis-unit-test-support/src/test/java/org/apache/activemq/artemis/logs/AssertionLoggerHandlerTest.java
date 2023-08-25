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
package org.apache.activemq.artemis.logs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.logs.AssertionLoggerHandler.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssertionLoggerHandlerTest {

   private static final String LOGGER_NAME = MethodHandles.lookup().lookupClass().getName();
   private static final Logger logger = LoggerFactory.getLogger(LOGGER_NAME);

   private static LogLevel origLevel;

   @BeforeClass
   public static void setLogLevel() {
      origLevel = AssertionLoggerHandler.setLevel(LOGGER_NAME, LogLevel.INFO);
   }

   @AfterClass
   public static void restoreLogLevel() throws Exception {
      AssertionLoggerHandler.setLevel(LOGGER_NAME, origLevel);
   }

   @Test
   public void testFindText() throws Exception {
      final String prefix = "123prefix";
      final String middle = "middle456";
      final String suffix = "suffix789";

      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler(true)) {
         // Try without logging anything
         assertFalse("should not have found prefix, not yet logged", loggerHandler.findText(prefix));
         assertFalse("should not have found middle, not yet logged", loggerHandler.findText(middle));
         assertFalse("should not have found suffix, not yet logged", loggerHandler.findText(suffix));
         assertFalse("should not have found combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));

         // Now log only the prefix.
         logger.info("{} -after", prefix);

         assertTrue("should have found prefix logged", loggerHandler.findText(prefix));
         assertFalse("should not have found middle, not yet logged", loggerHandler.findText(middle));
         assertFalse("should not have found suffix, not yet logged", loggerHandler.findText(suffix));
         assertFalse("should not have found combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));

         // Now log only the middle.
         logger.info("before- {} -after", middle);

         assertTrue("should have found prefix logged", loggerHandler.findText(prefix));
         assertTrue("should have found middle logged", loggerHandler.findText(middle));
         assertFalse("should not have found suffix, not yet logged", loggerHandler.findText(suffix));
         assertFalse("should not have found full combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));

         // Now log only the suffix.
         logger.info("before- {}", suffix);

         assertTrue("should have found prefix logged", loggerHandler.findText(prefix));
         assertTrue("should have found middle logged", loggerHandler.findText(middle));
         assertTrue("should have found suffix logged", loggerHandler.findText(suffix));
         assertFalse("should not have found alternative combination, not yet logged", loggerHandler.findText(prefix, suffix));
         assertFalse("should not have found alternative combination, not yet logged", loggerHandler.findText(prefix, middle));
         assertFalse("should not have found alternative combination, not yet logged", loggerHandler.findText(middle, suffix));
         assertFalse("should not have found full combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));
      }

      // Use a new AssertionLoggerHandler to start fresh
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler(true)) {
         // Try again without logging anything
         assertFalse("should not have found prefix, not yet logged", loggerHandler.findText(prefix));
         assertFalse("should not have found middle, not yet logged", loggerHandler.findText(middle));
         assertFalse("should not have found suffix, not yet logged", loggerHandler.findText(suffix));
         assertFalse("should not have found full combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));

         // Now log the prefix AND suffix, but NOT middle.
         logger.info("{} -inbetween- {}", prefix, suffix);

         assertTrue("should have found prefix logged", loggerHandler.findText(prefix));
         assertFalse("should not have found middle, not yet logged", loggerHandler.findText(middle));
         assertTrue("should have found suffix logged", loggerHandler.findText(suffix));
         assertTrue("should have found combination logged", loggerHandler.findText(prefix, suffix));
         assertFalse("should not have found alternative combination, not yet logged", loggerHandler.findText(prefix, middle));
         assertFalse("should not have found alternative combination, not yet logged", loggerHandler.findText(middle, suffix));
         assertFalse("should not have found full combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));
      }

      // Use a new AssertionLoggerHandler to start fresh
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler(true)) {
         // Try again without logging anything
         assertFalse("should not have found prefix, not yet logged", loggerHandler.findText(prefix));
         assertFalse("should not have found middle, not yet logged", loggerHandler.findText(middle));
         assertFalse("should not have found suffix, not yet logged", loggerHandler.findText(suffix));
         assertFalse("should not have found combination, not yet logged", loggerHandler.findText(prefix, middle, suffix));

         // Now log the prefix AND middle AND suffix.
         logger.info("{} - {} - {}", prefix, middle, suffix);

         assertTrue("should have found prefix logged", loggerHandler.findText(prefix));
         assertTrue("should have found middle logged", loggerHandler.findText(middle));
         assertTrue("should have found suffix logged", loggerHandler.findText(suffix));
         assertTrue("should have found combination logged", loggerHandler.findText(prefix, suffix));
         assertTrue("should have found combination logged", loggerHandler.findText(prefix, middle));
         assertTrue("should have found combination logged", loggerHandler.findText(middle, suffix));
         assertTrue("should have found full combination logged", loggerHandler.findText(prefix, middle, suffix));
      }
   }

}
