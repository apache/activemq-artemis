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

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class LoginPage extends ConsolePage {
   private By brandLocator = By.xpath("//img[@class='pf-c-brand']");
   private By usernameLocator = By.id("username");
   private By passwordLocator = By.id("password");
   private By loginButtonLocator = By.xpath("//button[@type='submit']");
   private By userDropdownMenuLocator = By.id("userDropdownMenu");

   public LoginPage(WebDriver driver) {
      super(driver);
   }

   public String getBrandImage(int timeout) {
      waitForElementToBeVisible(brandLocator, timeout);

      return driver.findElement(brandLocator).getAttribute("src");
   }

   public StatusPage loginValidUser(String username, String password, int timeout) {
      waitForElementToBeVisible(usernameLocator, timeout);
      waitForElementToBeVisible(passwordLocator, timeout);
      waitForElementToBeClickable(loginButtonLocator, timeout);

      driver.findElement(usernameLocator).sendKeys(username);
      driver.findElement(passwordLocator).sendKeys(password);
      driver.findElement(loginButtonLocator).click();

      waitForElementToBeVisible(userDropdownMenuLocator, timeout);
      waitForElementToBeVisible(By.xpath("//a[contains(text(),'Status')]"), timeout);

      return new StatusPage(driver);
   }
}
