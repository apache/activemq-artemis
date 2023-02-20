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
package org.apache.activemq.artemis.tests.smoke.console.pages;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.artemis.utils.StringEscapeUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
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

   public SendMessagePage getAddressSendMessagePage(String address, int timeout) {
      driver.get(getServerUrl() + "/console/artemis/artemisAddressSendMessage?nid=root-org.apache.activemq.artemis-0.0.0.0-addresses-" + address);

      waitForElementToBeVisible(By.xpath("//h1[contains(text(),'Send Message')]"), timeout);

      return new SendMessagePage(driver);
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

   public LoginPage logout(int timeout) {
      WebElement logoutWebElement = driver.findElement(logoutLocator);
      WebElement userDropdownMenuWebElement = driver.findElement(userDropdownMenuLocator);

      if (!logoutWebElement.isDisplayed()) {
         userDropdownMenuWebElement.click();
      }

      logoutWebElement.click();

      return new LoginPage(driver);
   }

   public Object postJolokiaExecRequest(String mbean, String operation, String arguments) {
      Object response = ((JavascriptExecutor) driver).executeAsyncScript(
         "var callback = arguments[arguments.length - 1];" +
            "var xhr = new XMLHttpRequest();" +
            "xhr.open('POST', '/console/jolokia', true);" +
            "xhr.onreadystatechange = function() {" +
            "  if (xhr.readyState == 4) {" +
            "    callback(xhr.responseText);" +
            "  }" +
            "};" +
            "xhr.send('{\"type\":\"exec\",\"mbean\":\"" + StringEscapeUtils.escapeString(StringEscapeUtils.escapeString(mbean)) +
            "\",\"operation\":\"" + StringEscapeUtils.escapeString(StringEscapeUtils.escapeString(operation)) +
            "\",\"arguments\":[" + StringEscapeUtils.escapeString(arguments) + "]}');");

      return response;
   }
}
