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

package org.apache.activemq.artemis.tests.smoke.console.pages;

import java.time.Duration;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public abstract class ConsolePage {
   protected WebDriver driver;

   private By titleLocator = By.tagName("h1");

   public ConsolePage(WebDriver driver) {
      this.driver = driver;
   }

   public void refresh(int timeout) {
      driver.navigate().refresh();

      waitForLoading(timeout);
   }

   public void waitForLoading(int timeout) {
      waitForElementToBeVisible(titleLocator, timeout);
   }

   public void waitForElementToBeVisible(final By elementLocator, int timeout) {
      WebDriverWait loadWebDriverWait = new WebDriverWait(
         driver, Duration.ofMillis(timeout).getSeconds());

      loadWebDriverWait.until(ExpectedConditions.visibilityOfElementLocated(elementLocator));
   }

   public void waitForElementToBeClickable(final By elementLocator, int timeout) {
      WebDriverWait loadWebDriverWait = new WebDriverWait(
         driver, Duration.ofMillis(timeout).getSeconds());

      loadWebDriverWait.until(ExpectedConditions.elementToBeClickable(elementLocator));
   }
}
