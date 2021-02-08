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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public abstract class ArtemisPage extends ConsolePage {
   private By logoutLocator = By.cssSelector("a[ng-focus='authService.logout()']");
   private By dropdownMenuLocator = By.id("moreDropdown");
   private By userDropdownMenuLocator = By.id("userDropdownMenu");
   private By queuesMenuItemLocator = By.xpath("//a[contains(text(),'Queues')]");

   public ArtemisPage(WebDriver driver) {
      super(driver);
   }

   public String getUser() {
      WebElement logoutWebElement = driver.findElement(logoutLocator);
      WebElement userDropdownMenuWebElement = driver.findElement(userDropdownMenuLocator);

      if (!logoutWebElement.isDisplayed()) {
         userDropdownMenuWebElement.click();
      }

      String logoutText = logoutWebElement.getText();
      Pattern pattern = Pattern.compile("Logout \\(([^\\)]+)\\)");
      Matcher matcher = pattern.matcher(logoutText);

      userDropdownMenuWebElement.click();

      if (matcher.find()) {
         return matcher.group(1);
      }

      return null;
   }

   public QueuesPage getQueuesPage(int timeout) {
      WebElement queuesMenuItem = driver.findElement(queuesMenuItemLocator);

      if (!queuesMenuItem.isDisplayed()) {
         List<WebElement> dropdownMenu = driver.findElements(dropdownMenuLocator);

         if (dropdownMenu.size() > 0) {
            dropdownMenu.get(0).click();
         } else {
            waitForElementToBeVisible(queuesMenuItemLocator, timeout);
         }
      }

      queuesMenuItem.click();

      waitForElementToBeVisible(By.xpath("//h1[contains(text(),'Browse Queues')]"), timeout);

      return new QueuesPage(driver);
   }
}
