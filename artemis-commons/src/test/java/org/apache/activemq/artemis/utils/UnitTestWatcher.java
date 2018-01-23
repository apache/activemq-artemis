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
package org.apache.activemq.artemis.utils;

import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Set;

public class UnitTestWatcher extends TestWatcher {

   //make it static to allow other rules in a chain
   //to register themselves without having a reference to this rule.
   public static Set<TestObserver> observers = new ConcurrentHashSet();

   @Override
   protected void succeeded(Description description) {
      try {
         for (TestObserver observer : observers) {
            observer.testSucceeded(description);
         }
      } finally {
         observers.clear();
      }
   }

   @Override
   protected void failed(Throwable e, Description description) {
      try {
         for (TestObserver observer : observers) {
            observer.testFailed(e, description);
         }
      } finally {
         observers.clear();
      }
   }

   public static void addObserver(TestObserver observer) {
      observers.add(observer);
   }

   public static void disableCheck() {
      observers.clear();
   }
}
