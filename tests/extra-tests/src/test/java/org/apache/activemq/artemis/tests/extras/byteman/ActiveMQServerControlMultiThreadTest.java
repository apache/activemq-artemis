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

package org.apache.activemq.artemis.tests.extras.byteman;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.integration.management.ManagementTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ActiveMQServerControlMultiThreadTest extends ManagementTestBase {

   private ActiveMQServer server;
   private static volatile CountDownLatch delayCalled;

   /**
    * Aim: verify that no exceptions will occur if deleteAddress() is invoked when listAddress() is happening
    *
    * test delays the listAddress() operations; delay after the list of addresses are retrieved but before
    * Json string (representing the addresses) is created.
    *
    * @throws Exception
    */

   @Test
   @BMRules(rules = {@BMRule(
      name = "Delay listAddress() by 2 secs ",
      targetClass = "org.apache.activemq.artemis.core.management.impl.view.AddressView ",
      targetMethod = "<init>(org.apache.activemq.artemis.core.server.ActiveMQServer)",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.ActiveMQServerControlMultiThreadTest.delay(2)")})

   public void listAddressDuringDeleteAddress() throws Exception {

      ExecutorService executorService = Executors.newFixedThreadPool(1);
      String addressName1 = "MyAddress_one";
      String addressName2 = "MyAddress_two";

      try {

         //used to block thread, until the delay() has been called.
         delayCalled = new CountDownLatch(1);

         ActiveMQServerControl serverControl = createManagementControl();

         serverControl.createAddress(addressName1, RoutingType.ANYCAST.toString());
         serverControl.createAddress(addressName2, RoutingType.ANYCAST.toString());

         executorService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  // wait until the listAddress has retrieved list of addresses BUT has not
                  // created JSon string.
                  delayCalled.await();
                  serverControl.deleteAddress(addressName1);
               } catch (Exception e) {
                  e.printStackTrace();
               }

            }
         });

         String filter = createJsonFilter("", "", "");
         String addressesAsJsonString = serverControl.listAddresses(filter, 1, 10);
         JsonObject addressesAsJsonObject = JsonUtil.readJsonObject(addressesAsJsonString);
         JsonArray addressesArray = (JsonArray) addressesAsJsonObject.get("data");

         // the deleteAddress() should have happened before the Json String was created
         Assert.assertEquals("number of Addresses returned from query", 1, addressesArray.size());
         Assert.assertEquals("check addresses username", addressName2.toString(), addressesArray.getJsonObject(0).getString("name"));

      } finally {
         executorService.shutdown();
      }
   }

   //notify delay has been called and wait for X seconds
   public static void delay(int seconds) {
      delayCalled.countDown();
      try {
         Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true);
      server = createServer(false, config);
      server.setMBeanServer(mbeanServer);
      server.start();

   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
      server.stop();

   }

   protected ActiveMQServerControl createManagementControl() throws Exception {
      return ManagementControlHelper.createActiveMQServerControl(mbeanServer);
   }

   private String createJsonFilter(String fieldName, String operationName, String value) {
      HashMap<String, Object> filterMap = new HashMap<>();
      filterMap.put("field", fieldName);
      filterMap.put("operation", operationName);
      filterMap.put("value", value);
      JsonObject jsonFilterObject = JsonUtil.toJsonObject(filterMap);
      return jsonFilterObject.toString();
   }

}