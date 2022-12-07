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

import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
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
import org.openqa.selenium.remote.RemoteWebDriver;
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
      // The tests can be executed using a remote server, local browsers or testcontainers[2].
      // To use a remote server set the `webdriver.remote.server` property with the URL
      // of the server, ie -Dwebdriver.remote.server=http://localhost:4444/wd/hub
      // To use your local Google Chrome browser download the WebDriver for Chrome[3] and set
      // the `webdriver.chrome.driver` property with the WebDriver path, ie
      // -Dwebdriver.chrome.driver=/home/developer/chromedriver_linux64/chromedriver
      // To use your local Firefox browser download the WebDriver for Firefox[4] and set
      // the `webdriver.gecko.driver` property with the WebDriver path, ie
      // -Dwebdriver.gecko.driver=/home/developer/geckodriver-v0.28.0-linux64/geckodriver
      // To use the testcontainers[2] install docker.
      //
      // [1] https://github.com/SeleniumHQ/selenium
      // [2] https://www.testcontainers.org/modules/webdriver_containers
      // [3] https://chromedriver.chromium.org/
      // [4] https://github.com/mozilla/geckodriver/

      try {
         String webdriverName;
         String webdriverLocation;
         String webdriverArguments;
         String webdriverRemoteServer;
         Function<MutableCapabilities, WebDriver> webDriverConstructor;
         BiConsumer<MutableCapabilities, String[]> webdriverArgumentsSetter;

         if (browserOptions instanceof ChromeOptions) {
            webdriverName = "chrome";
            webDriverConstructor = browserOptions -> new ChromeDriver((ChromeOptions)browserOptions);
            webdriverArgumentsSetter = (browserOptions, arguments) -> ((ChromeOptions) browserOptions).addArguments(arguments);
         } else if (browserOptions instanceof FirefoxOptions) {
            webdriverName = "gecko";
            webDriverConstructor = browserOptions -> new FirefoxDriver((FirefoxOptions)browserOptions);
            webdriverArgumentsSetter = (browserOptions, arguments) -> ((FirefoxOptions) browserOptions).addArguments(arguments);
         } else {
            throw new IllegalStateException("Unexpected browserOptions: " + browserOptions);
         }

         webdriverArguments = System.getProperty("webdriver." + webdriverName + ".driver.args");
         if (webdriverArguments != null) {
            webdriverArgumentsSetter.accept(browserOptions, webdriverArguments.split(","));
         }

         webdriverLocation = System.getProperty("webdriver." + webdriverName + ".driver");

         webdriverRemoteServer = System.getProperty("webdriver." + webdriverName + ".remote.server");
         if (webdriverRemoteServer == null) {
            webdriverRemoteServer = System.getProperty("webdriver.remote.server");
         }

         if (webdriverRemoteServer != null) {
            driver = new RemoteWebDriver(new URL(webdriverRemoteServer), browserOptions);
         } else if (webdriverLocation != null) {
            driver = webDriverConstructor.apply(browserOptions);
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
            webDriver.get(serverUrl);
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
