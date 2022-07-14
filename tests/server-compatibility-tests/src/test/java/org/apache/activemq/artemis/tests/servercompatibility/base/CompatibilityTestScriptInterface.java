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

package org.apache.activemq.artemis.tests.servercompatibility.base;

import groovy.lang.Closure;

import java.io.File;

@SuppressWarnings("unused")
public interface CompatibilityTestScriptInterface {

   File getWorkingDir();
   String getSide();
   void assertEquals(Object expected, Object given);
   void assertEquals(String message, Object expected, Object given);
   void assertTrue(boolean condition);
   void assertTrue(String message, boolean condition);
   void assertFalse(boolean condition);
   void assertFalse(String message, boolean condition);
   void assertNotNull(Object object);
   void assertNotNull(String message, Object object);
   void waitForCondition(String failMessage, int seconds, Closure<Boolean> condition) throws InterruptedException;
   void waitForCondition(String failMessage, String waitMessage, int seconds, Closure<Boolean> condition) throws InterruptedException;
   void fail();
   void fail(String message);
}
