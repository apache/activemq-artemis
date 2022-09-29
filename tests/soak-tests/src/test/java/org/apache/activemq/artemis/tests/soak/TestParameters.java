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

package org.apache.activemq.artemis.tests.soak;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/** Encapsulates System properties that could be passed on to the test. */
public class TestParameters {


   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String propertyName(String testName, String property) {
      if (testName == null) {
         return "TEST_" + property;
      } else {
         return "TEST_" + testName + "_" + property;
      }
   }

   public static int testProperty(String testName, String property, int defaultValue) {
      try {
         return Integer.parseInt(testProperty(testName, property, Integer.toString(defaultValue)));
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         return defaultValue;
      }
   }

   public static int intMandatoryProperty(String testName, String property) {
      try {
         return Integer.parseInt(mandatoryProperty(testName, property));
      } catch (Throwable e) {
         Assert.fail(e.getMessage());
         return -1; // never happening, just to make it compile
      }
   }

   public static String testProperty(String testName, String property, String defaultValue) {

      property = propertyName(testName, property);

      String value = System.getenv(property);
      if (value == null) {
         value = System.getProperty(property);
      }

      if (value == null) {
         logger.debug("System property '{}' not defined, using default: {}", property, defaultValue);
         value = defaultValue;
      }

      logger.info("{}={}", property, value);

      return value;

   }


   public static String mandatoryProperty(String testName, String property) {
      property = propertyName(testName, property);

      String value = System.getenv(property);
      if (value == null) {
         value = System.getProperty(property);
      }

      if (value == null) {
         Assert.fail("mandatory System property '" + property + "' not defined");
      }

      logger.info("{}={}", property, value);

      return value;
   }



}
