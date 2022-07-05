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
import org.openqa.selenium.WebElement;

import java.util.List;

public class QueuePage extends ArtemisPage {
   private By messageRowLocator = By.cssSelector("tr[role='row'][class='ng-scope odd']");

   public QueuePage(WebDriver driver) {
      super(driver);
   }

   public MessagePage getMessagePage(int index, int timeout) {
      driver.findElements(By.cssSelector("button[title='Show message']")).get(index).click();

      waitForElementToBeVisible(By.cssSelector("span[role='presentation']"), timeout);

      return new MessagePage(driver);
   }

   public long getMessageId(int index) {
      WebElement messageRowWebElement = driver.findElements(messageRowLocator).get(index);

      String messageIdText = messageRowWebElement.findElements(By.tagName("td")).get(
         getIndexOfColumn("Message ID")).findElement(By.tagName("span")).getText();

      return Long.parseLong(messageIdText);
   }

   public String getMessageOriginalQueue(int index) {
      WebElement messageRowWebElement = driver.findElements(messageRowLocator).get(index);

      String messageOriginalQueue = messageRowWebElement.findElements(By.tagName("td")).get(
         getIndexOfColumn("Original Queue")).findElement(By.tagName("span")).getText();

      return messageOriginalQueue;
   }

   public int getIndexOfColumn(String name) {
      WebElement headerRowWebElement = driver.findElement(By.cssSelector("tr[role='row']"));

      List<WebElement> columnWebElements = headerRowWebElement.findElements(By.tagName("th"));
      for (int i = 0; i < columnWebElements.size(); i++) {
         if (name.equals(columnWebElements.get(i).getText())) {
            return i;
         }
      }

      return -1;
   }
}
