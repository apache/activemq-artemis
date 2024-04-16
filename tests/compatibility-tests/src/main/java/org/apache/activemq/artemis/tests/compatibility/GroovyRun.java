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

package org.apache.activemq.artemis.tests.compatibility;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

public class GroovyRun {

   public static final String SNAPSHOT = "ARTEMIS-SNAPSHOT";
   public static final String JAKARTAEE = "ARTEMIS-JAKARTAEE";
   public static final String ONE_FIVE = "ARTEMIS-155";
   public static final String ONE_FOUR = "ARTEMIS-140";
   public static final String TWO_ZERO = "ARTEMIS-200";
   public static final String TWO_ONE = "ARTEMIS-210";
   public static final String TWO_FOUR = "ARTEMIS-240";
   public static final String TWO_SIX_THREE = "ARTEMIS-263";
   public static final String TWO_SEVEN_ZERO = "ARTEMIS-270";
   public static final String TWO_TEN_ZERO = "ARTEMIS-2_10_0";
   public static final String TWO_SEVENTEEN_ZERO = "ARTEMIS-2_17_0";
   public static final String TWO_EIGHTEEN_ZERO = "ARTEMIS-2_18_0";
   public static final String TWO_TWENTYTWO_ZERO = "ARTEMIS-2_22_0";
   public static final String TWO_TWENTYEIGHT_ZERO = "ARTEMIS-2_28_0";
   public static final String TWO_THIRTYTHREE_ZERO = "ARTEMIS-2_33_0";
   public static final String HORNETQ_235 = "HORNETQ-235";
   public static final String HORNETQ_247 = "HORNETQ-247";
   public static final String AMQ_5_11 = "AMQ_5_11";

   public static Binding binding = new Binding();
   public static GroovyShell shell = new GroovyShell(binding);

   public static void clear() {
      List<String> variablesToRemove = new ArrayList<>();
      variablesToRemove.addAll(binding.getVariables().keySet());
      variablesToRemove.forEach(v -> binding.removeVariable(v));
   }

   /**
    * This can be called from the scripts as well.
    *  The scripts will use this method instead of its own groovy method.
    *  As a classloader operation needs to be done here.
    */
   public static Object evaluate(String script,
                                 String[] arg) throws URISyntaxException, IOException {
      return evaluate(script, "arg", arg);
   }

      /**
       * This can be called from the scripts as well.
       *  The scripts will use this method instead of its own groovy method.
       *  As a classloader operation needs to be done here.
       */
   public static Object evaluate(String script,
                               String argVariableName,
                               String[] arg) throws URISyntaxException, IOException {
      URL scriptURL = GroovyRun.class.getClassLoader().getResource(script);
      if (scriptURL == null) {
         throw new RuntimeException("cannot find " + script);
      }
      URI scriptURI = scriptURL.toURI();

      setVariable(argVariableName, arg);

      return shell.evaluate(scriptURI);
   }


   public static void setVariable(String name, Object arg) {
      binding.setVariable(name, arg);
   }

   public static Object getVariable(String name) {
      return binding.getVariable(name);
   }

   // Called with reflection
   public static Object execute(String script) throws Throwable {
      return shell.evaluate(script);
   }

   public static void assertNotNull(Object value) {
      if (value == null) {
         throw new RuntimeException("Null value");
      }
   }

   public static void assertNull(Object value) {
      if (value != null) {
         throw new RuntimeException("Expected Null value");
      }
   }

   public static void assertTrue(boolean  value) {
      if (!value) {
         throw new RuntimeException("Expected true");
      }
   }


   public static void assertFalse(boolean  value) {
      if (value) {
         throw new RuntimeException("Expected false");
      }
   }

   public static void assertEquals(Object value1, Object value2) {
      if ((value1 == null && value2 == null) || !value1.equals(value2)) {
         throw new RuntimeException(value1 + "!=" + value2);
      }
   }

   public static void assertEquals(int value1, int value2) {
      if (value1 != value2) {
         throw new RuntimeException(value1 + "!=" + value2);
      }
   }

   public static void assertEquals(byte[] value1, byte[] value2) {

      assertEquals(value1.length, value2.length);

      for (int i = 0; i < value1.length; i++) {
         assertEquals(value1[i], value2[i]);
      }
   }


   public static byte getSamplebyte(final long position) {
      return (byte) ('a' + position % ('z' - 'a' + 1));
   }
}

