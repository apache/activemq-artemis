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

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.interactions.Actions;

public class SendMessagePage extends ArtemisPage {
   private By messageTextLocator = By.cssSelector("span[role='presentation']");
   private By messageTextEditorLocator = By.cssSelector("div[hawtio-editor='$ctrl.message.message']");

   public SendMessagePage(WebDriver driver) {
      super(driver);
   }

   public void clearMessageText() {
      while (!getMessageText().isEmpty()) {
         Actions actions = new Actions(driver);
         actions.click(driver.findElement(messageTextEditorLocator));
         actions.sendKeys(Keys.BACK_SPACE);
         actions.sendKeys(Keys.DELETE);
         actions.perform();
      }
   }

   public boolean isUseCurrentLogonUserSelected() {
      return driver.findElement(By.id("useCurrentLogonUser")).isSelected();
   }


   public void selectUseCurrentLogonUser() {
      if (!isUseCurrentLogonUserSelected()) {
         driver.findElement(By.id("useCurrentLogonUser")).click();
      }
   }

   public void unselectUseCurrentLogonUser() {
      if (isUseCurrentLogonUserSelected()) {
         driver.findElement(By.id("useCurrentLogonUser")).click();
      }
   }

   public void appendMessageText(String text) {
      Actions actions = new Actions(driver);
      actions.click(driver.findElement(messageTextEditorLocator));
      actions.sendKeys(text);
      actions.perform();
   }

   public String getMessageText() {
      return driver.findElement(messageTextLocator).getText();
   }

   public void sendMessage() {
      driver.findElement(By.xpath("//button[contains(text(),'Send Message')]")).click();

      driver.findElement(By.xpath("//button[@class='close']")).click();
   }
}
