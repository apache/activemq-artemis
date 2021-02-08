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

public class QueuesPage extends ArtemisPage {
   private static final int MESSAGES_COUNT_COLUMN = 10;

   public QueuesPage(WebDriver driver) {
      super(driver);
   }

   public QueuePage getQueuePage(String name, int timeout) {
      WebElement messagesCountWebElement = driver.findElement(getQueueLocator(name)).
         findElements(By.tagName("td")).get(MESSAGES_COUNT_COLUMN);

      messagesCountWebElement.findElement(By.tagName("a")).click();

      waitForElementToBeVisible(By.xpath("//h1[contains(text(),'Browse Queue')]"), timeout);

      return new QueuePage(driver);
   }

   public int countQueue(String name) {
      return driver.findElements(getQueueLocator(name)).size();
   }

   public int getMessagesCount(String name) {
      WebElement messagesCountWebElement = driver.findElement(getQueueLocator(name)).
         findElements(By.tagName("td")).get(MESSAGES_COUNT_COLUMN);

      return Integer.parseInt(messagesCountWebElement.findElement(By.tagName("a")).getText());
   }

   private By getQueueLocator(String name) {
      return By.xpath("//tr[td/span/a='" + name + "']");
   }
}
