/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.web;

import java.util.Properties;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.component.WebServerComponent;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.BindingDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.apache.activemq.artemis.json.JsonArray;
import org.apache.activemq.artemis.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class WebServerDTOConfigTest {

   private static final String INVALID_ATTRIBUTE_NAME = "invalidAttribute";
   private static final String BINDING_TEST_NAME = "test-binding";
   private static final String BINDING_TEST_URL = "http://localhost:61616";
   private static final String APP_TEST_NAME = "test-app";
   private static final String APP_TEST_URL = "test-url";

   @Test
   public void testSetWebProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      Properties properties = new Properties();
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "customizer", "customizerTest");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "rootRedirectLocation", "locationTest");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "webContentEnabled", "true");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + INVALID_ATTRIBUTE_NAME, "true");
      Configuration configuration = new ConfigurationImpl();
      String systemWebPropertyPrefix = ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix();
      configuration.parsePrefixedProperties(webServer, "system-" + systemWebPropertyPrefix, properties, systemWebPropertyPrefix);

      Assert.assertEquals("customizerTest", webServer.getCustomizer());
      Assert.assertEquals("locationTest", webServer.getRootRedirectLocation());
      Assert.assertEquals(true, webServer.getWebContentEnabled());

      testStatus(configuration.getStatus(), "system-" + systemWebPropertyPrefix, "");
   }

   @Test
   public void testSetNewWebBindingProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();

      testSetWebBindingProperties(webServer, BINDING_TEST_NAME);
   }

   @Test
   public void testSetExistingWebBindingProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      BindingDTO exitingBinding = new BindingDTO();
      exitingBinding.setName(BINDING_TEST_NAME);
      webServer.addBinding(exitingBinding);

      testSetWebBindingProperties(webServer, BINDING_TEST_NAME);
   }

   @Test
   public void testSetExistingWebBindingWithoutNameProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      BindingDTO exitingBinding = new BindingDTO();
      exitingBinding.setUri(BINDING_TEST_URL);
      webServer.addBinding(exitingBinding);

      testSetWebBindingProperties(webServer, BINDING_TEST_URL);
   }

   private void testSetWebBindingProperties(WebServerDTO webServer, String bindingName) throws Throwable {
      Properties properties = new Properties();
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".uri", BINDING_TEST_URL);
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".clientAuth", "true");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".passwordCodec", "test-passwordCodec");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".keyStorePath", "test-keyStorePath");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".trustStorePath", "test-trustStorePath");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".includedTLSProtocols", "test-includedTLSProtocols,0");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".excludedTLSProtocols", "test-excludedTLSProtocols,1");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".includedCipherSuites", "test-includedCipherSuites,2");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".excludedCipherSuites", "test-excludedCipherSuites,3");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".keyStorePassword", "test-keyStorePassword");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".trustStorePassword", "test-trustStorePassword");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".sniHostCheck", !WebServerComponent.DEFAULT_SNI_HOST_CHECK_VALUE);
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".sniRequired", !WebServerComponent.DEFAULT_SNI_REQUIRED_VALUE);
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + "." + INVALID_ATTRIBUTE_NAME, "true");
      Configuration configuration = new ConfigurationImpl();
      String systemWebPropertyPrefix = ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix();
      configuration.parsePrefixedProperties(webServer, "system-" + systemWebPropertyPrefix, properties, systemWebPropertyPrefix);

      BindingDTO testBinding = webServer.getAllBindings().stream().filter(binding -> bindingName.equals(binding.getName())).findFirst().get();
      Assert.assertEquals(BINDING_TEST_URL, testBinding.getUri());
      Assert.assertEquals(true, testBinding.getClientAuth());
      Assert.assertEquals("test-passwordCodec", testBinding.getPasswordCodec());
      Assert.assertEquals("test-keyStorePath", testBinding.getKeyStorePath());
      Assert.assertEquals("test-trustStorePath", testBinding.getTrustStorePath());
      Assert.assertEquals("test-includedTLSProtocols,0", String.join(",", testBinding.getIncludedTLSProtocols()));
      Assert.assertEquals("test-excludedTLSProtocols,1", String.join(",", testBinding.getExcludedTLSProtocols()));
      Assert.assertEquals("test-includedCipherSuites,2", String.join(",", testBinding.getIncludedCipherSuites()));
      Assert.assertEquals("test-excludedCipherSuites,3", String.join(",", testBinding.getExcludedCipherSuites()));
      Assert.assertEquals("test-keyStorePassword", testBinding.getKeyStorePassword());
      Assert.assertEquals("test-trustStorePassword", testBinding.getTrustStorePassword());
      Assert.assertEquals(!WebServerComponent.DEFAULT_SNI_HOST_CHECK_VALUE, testBinding.getSniHostCheck());
      Assert.assertEquals(!WebServerComponent.DEFAULT_SNI_REQUIRED_VALUE, testBinding.getSniRequired());

      testStatus(configuration.getStatus(), "system-" + systemWebPropertyPrefix, "bindings." + bindingName + ".");
   }

   @Test
   public void testSetNewWebBindingAppProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();

      testSetWebBindingAppProperties(webServer, BINDING_TEST_NAME, APP_TEST_NAME);
   }

   @Test
   public void testSetExistingWebBindingAppProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      BindingDTO exitingBinding = new BindingDTO();
      exitingBinding.setName(BINDING_TEST_NAME);
      AppDTO existingApp = new AppDTO();
      existingApp.setName(APP_TEST_NAME);
      exitingBinding.addApp(existingApp);
      webServer.addBinding(exitingBinding);

      testSetWebBindingAppProperties(webServer, BINDING_TEST_NAME, APP_TEST_NAME);
   }

   @Test
   public void testSetExistingWebBindingAppWithoutNameProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      BindingDTO exitingBinding = new BindingDTO();
      exitingBinding.setName(BINDING_TEST_NAME);
      AppDTO existingApp = new AppDTO();
      existingApp.setUrl(APP_TEST_URL);
      exitingBinding.addApp(existingApp);
      webServer.addBinding(exitingBinding);

      testSetWebBindingAppProperties(webServer, BINDING_TEST_NAME, APP_TEST_URL);
   }

   @Test
   public void testSetRequestLogProperties() throws Throwable {
      WebServerDTO webServer = new WebServerDTO();
      Properties properties = new Properties();
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.filename", "filenameTest");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.append", "true");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.extended", "true");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.filenameDateFormat", "filenameDateFormatTest");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.retainDays", "3");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog.ignorePaths", "ignorePathTest0,ignorePathTest1,ignorePathTest2");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "requestLog." + INVALID_ATTRIBUTE_NAME, "true");
      Configuration configuration = new ConfigurationImpl();
      String systemWebPropertyPrefix = ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix();
      configuration.parsePrefixedProperties(webServer, "system-" + systemWebPropertyPrefix, properties, systemWebPropertyPrefix);

      Assert.assertEquals("filenameTest", webServer.getRequestLog().getFilename());
      Assert.assertEquals(true, webServer.getRequestLog().getAppend());
      Assert.assertEquals(true, webServer.getRequestLog().getExtended());
      Assert.assertEquals("filenameDateFormatTest", webServer.getRequestLog().getFilenameDateFormat());
      Assert.assertEquals(Integer.valueOf(3), webServer.getRequestLog().getRetainDays());
      Assert.assertEquals("ignorePathTest0,ignorePathTest1,ignorePathTest2", webServer.getRequestLog().getIgnorePaths());

      testStatus(configuration.getStatus(), "system-" + systemWebPropertyPrefix, "requestLog.");
   }

   private void testSetWebBindingAppProperties(WebServerDTO webServer, String bindingName, String appName) throws Throwable {
      Properties properties = new Properties();
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".apps." + appName + ".url", APP_TEST_URL);
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".apps." + appName + ".war", "test-war");
      properties.put(ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix() + "bindings." + bindingName + ".apps." + appName + "." + INVALID_ATTRIBUTE_NAME, "true");
      Configuration configuration = new ConfigurationImpl();
      String systemWebPropertyPrefix = ActiveMQDefaultConfiguration.getDefaultSystemWebPropertyPrefix();
      configuration.parsePrefixedProperties(webServer, "system-" + systemWebPropertyPrefix, properties, systemWebPropertyPrefix);

      BindingDTO testBinding = webServer.getAllBindings().stream().filter(binding -> bindingName.equals(binding.getName())).findFirst().get();
      AppDTO testApp = testBinding.getApps().stream().filter(app -> appName.equals(app.getName())).findFirst().get();
      Assert.assertEquals("test-url", testApp.getUrl());
      Assert.assertEquals("test-war", testApp.getWar());

      testStatus(configuration.getStatus(), "system-" + systemWebPropertyPrefix, "bindings." + bindingName + ".apps." + appName + ".");
   }

   private void testStatus(String status, String name, String prefix) {
      Assert.assertNotNull(status);
      JsonObject statusJsonObject = JsonUtil.readJsonObject(status);
      Assert.assertNotNull(statusJsonObject);
      JsonObject propertiesJsonObject = statusJsonObject.getJsonObject("properties");
      Assert.assertNotNull(propertiesJsonObject);
      JsonObject systemWebPropertiesJsonObject = propertiesJsonObject.getJsonObject(name);
      Assert.assertNotNull(systemWebPropertiesJsonObject);
      JsonArray errorsJsonObject = systemWebPropertiesJsonObject.getJsonArray("errors");
      Assert.assertNotNull(errorsJsonObject);
      Assert.assertEquals(prefix + INVALID_ATTRIBUTE_NAME + "=true", errorsJsonObject.getJsonObject(0).getString("value"));
   }
}
