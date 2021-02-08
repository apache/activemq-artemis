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

package org.apache.activemq.artemis.tests.smoke.console;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openqa.selenium.MutableCapabilities;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BrowserWebDriverContainer;

@RunWith(Parameterized.class)
public abstract class ConsoleTest extends SmokeTestBase {
   protected static final String SERVER_NAME = "console";
   protected static final String SERVER_ADMIN_USERNAME = "admin";
   protected static final String SERVER_ADMIN_PASSWORD = "admin";

   protected static final int DEFAULT_TIMEOUT = 10000;
   protected static final String DEFAULT_SERVER_URL = "http://localhost:8161";
   protected static final String DEFAULT_CONTAINER_SERVER_URL = "http://host.testcontainers.internal:8161";
   protected static final String DEFAULT_CONSOLE_BRAND_IMAGE = "/activemq-branding/plugin/img/activemq.png";

   protected WebDriver driver;
   protected MutableCapabilities browserOptions;
   protected String serverUrl = DEFAULT_SERVER_URL;
   private BrowserWebDriverContainer browserWebDriverContainer;

   @Parameterized.Parameters(name = "browserOptions={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{new ChromeOptions()}, {new FirefoxOptions()}});
   }

   public ConsoleTest(MutableCapabilities browserOptions) {
      this.browserOptions = browserOptions;
   }

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME);
      disableCheckThread();
      startServer(SERVER_NAME, 0, 0);
      ServerUtil.waitForServerToStart(0, SERVER_ADMIN_USERNAME, SERVER_ADMIN_PASSWORD, 30000);


      // The ConsoleTest checks the web console using the selenium framework[1].
      // The tests can be executed using the local browsers or the webdriver testcontainers[2].
      // To use your local Google Chrome browser download the WebDriver for Chrome[3] and set
      // the `webdriver.chrome.driver` property with the WebDriver path, ie
      // -Dwebdriver.chrome.driver=/home/developer/chromedriver_linux64/chromedriver
      // To use your local Firefox browser download the WebDriver for Firefox[4] and set
      // the `webdriver.gecko.driver` property with the WebDriver path, ie
      // -Dwebdriver.gecko.driver=/home/developer/geckodriver-v0.28.0-linux64/geckodriver
      // To use the webdriver testcontainers[2] install docker.
      //
      // [1] https://github.com/SeleniumHQ/selenium
      // [2] https://www.testcontainers.org/modules/webdriver_containers
      // [3] https://chromedriver.chromium.org/
      // [4] https://github.com/mozilla/geckodriver/

      try {
         if (ChromeOptions.class.equals(browserOptions.getClass()) &&
            System.getProperty("webdriver.chrome.driver") != null) {
            driver = new ChromeDriver((ChromeOptions)browserOptions);
         } else if (FirefoxOptions.class.equals(browserOptions.getClass()) &&
            System.getProperty("webdriver.gecko.driver") != null) {
            driver = new FirefoxDriver((FirefoxOptions)browserOptions);
         } else {
            serverUrl = DEFAULT_CONTAINER_SERVER_URL;
            Testcontainers.exposeHostPorts(8161);
            browserWebDriverContainer = new BrowserWebDriverContainer().withCapabilities(this.browserOptions);
            browserWebDriverContainer.start();
            driver = browserWebDriverContainer.getWebDriver();
         }
      } catch (Exception e) {
         Assume.assumeNoException("Error on loading the web driver", e);
      }

      // Wait for server console
      WebDriverWait loadWebDriverWait = new WebDriverWait(
         driver, Duration.ofMillis(30000).getSeconds());

      loadWebDriverWait.until((Function<WebDriver, Object>) webDriver -> {
         try {
            webDriver.get(serverUrl + "/console");
            return true;
         } catch (Exception ignore) {
            return false;
         }
      });
   }

   @After
   public void stopWebDriver() {
      if (browserWebDriverContainer != null) {
         browserWebDriverContainer.stop();
      } else if (driver != null) {
         driver.close();
      }
   }
}
