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

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class QueuesPage extends ArtemisPage {
   private static final String MESSAGE_COUNT_COLUMN_NAME = "Message Count";

   public QueuesPage(WebDriver driver) {
      super(driver);
   }

   public QueuePage getQueuePage(String name, int timeout) {
      WebElement queueRowWebElement = driver.findElement(getQueueLocator(name));

      WebElement messagesCountWebElement = queueRowWebElement.findElements(By.tagName("td"))
         .get(getIndexOfColumn(MESSAGE_COUNT_COLUMN_NAME)).findElement(By.tagName("span"))
         .findElement(By.tagName("a"));

      messagesCountWebElement.click();

      waitForElementToBeVisible(By.xpath("//h1[contains(text(),'Browse Queue')]"), timeout);

      return new QueuePage(driver);
   }

   public int countQueue(String name) {
      return driver.findElements(getQueueLocator(name)).size();
   }

   public int getMessagesCount(String name) {
      WebElement queueRowWebElement = driver.findElement(getQueueLocator(name));

      String messageCountText = queueRowWebElement.findElements(By.tagName("td"))
         .get(getIndexOfColumn(MESSAGE_COUNT_COLUMN_NAME)).findElement(By.tagName("span"))
         .findElement(By.tagName("a")).getText();

      return Integer.parseInt(messageCountText);
   }

   private By getQueueLocator(String name) {
      return By.xpath("//tr[td/span/a='" + name + "']");
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
