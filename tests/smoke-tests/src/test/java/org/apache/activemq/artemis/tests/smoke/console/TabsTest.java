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
package org.apache.activemq.artemis.tests.smoke.console;

import org.apache.activemq.artemis.tests.smoke.console.pages.LoginPage;
import org.apache.activemq.artemis.utils.RetryRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openqa.selenium.By;
import org.openqa.selenium.MutableCapabilities;
import org.openqa.selenium.NoSuchElementException;

@RunWith(Parameterized.class)
public class TabsTest extends ConsoleTest {

   @Rule
   public RetryRule retryRule = new RetryRule(2);

   public TabsTest(MutableCapabilities browserOptions) {
      super(browserOptions);
   }

   @Test
   public void testConnectionsTab() {
      testTab("connections", "Connections");
   }

   @Test
   public void testSessionsTab() {
      testTab("sessions", "Sessions");
   }

   @Test
   public void testConsumersTab() {
      testTab("consumers", "Consumers");
   }

   @Test
   public void testProducersTab() {
      testTab("producers", "Producers");
   }

   @Test
   public void testAddressesTab() {
      testTab("addresses", "Addresses");
   }

   @Test
   public void testQueuesTab() {
      testTab("queues", "Queues");
   }

   private void testTab(String userpass, String tab) {
      driver.get(webServerUrl + "/console");
      new LoginPage(driver).loginValidUser(userpass, userpass, DEFAULT_TIMEOUT);
      driver.findElement(By.xpath("//a[contains(text(),'" + tab + "')]"));
   }

   @Test
   public void testConnectionsTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("queues", "Connections");
   }

   @Test
   public void testSessionsTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("connections", "Sessions");
   }

   @Test
   public void testConsumersTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("connections", "Consumers");
   }

   @Test
   public void testProducersTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("connections", "roducers");
   }

   @Test
   public void testAddressesTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("connections", "Addresses");
   }

   @Test
   public void testQueuesTabNegative() {
      // use credentials for a valid user who cannot see the tab
      testTabNegative("connections", "Queues");
   }

   private void testTabNegative(String userpass, String tab) {
      driver.get(webServerUrl + "/console");
      new LoginPage(driver).loginValidUser(userpass, userpass, DEFAULT_TIMEOUT);
      try {
         driver.findElement(By.xpath("//a[contains(text(),'" + tab + "')]"));
         fail("User " + userpass + " should not have been able to see the " + tab + " tab.");
      } catch (NoSuchElementException e) {
         // expected
      }
   }
}
