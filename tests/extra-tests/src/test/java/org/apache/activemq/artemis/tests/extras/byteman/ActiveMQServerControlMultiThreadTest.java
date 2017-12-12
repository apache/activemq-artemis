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
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
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
   @BMRules(rules = {@BMRule(name = "Delay listAddress() by 2 secs ",
      targetClass = "org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl",
      targetMethod = "getAddressInfo(org.apache.activemq.artemis.api.core.SimpleString)",
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

   /**
    * Aim: verify that no exceptions will occur when a session is closed during listConsumers() operation
    *
    * test delays the listConsumer() BEFORE the Session information associated with the consumer is retrieved.
    * During this delay the client session is closed.
    *
    * @throws Exception
    */

   @Test
   @BMRules(rules = {@BMRule(name = "Delay listConsumers() by 2 secs ",
      targetClass = "org.apache.activemq.artemis.core.management.impl.view.ConsumerView",
      targetMethod = "toJson(org.apache.activemq.artemis.core.server.ServerConsumer)",
      targetLocation = "ENTRY",
      action = "org.apache.activemq.artemis.tests.extras.byteman.ActiveMQServerControlMultiThreadTest.delay(2)")})

   public void listConsumersDuringSessionClose() throws Exception {

      ExecutorService executorService = Executors.newFixedThreadPool(1);
      SimpleString addressName1 = new SimpleString("MyAddress_one");
      SimpleString queueName1 = new SimpleString("my_queue_one");

      ActiveMQServerControl serverControl = createManagementControl();

      server.addAddressInfo(new AddressInfo(addressName1, RoutingType.ANYCAST));
      server.createQueue(addressName1, RoutingType.ANYCAST, queueName1, null, false, false);

      // create a consumer
      try (ServerLocator locator = createInVMNonHALocator(); ClientSessionFactory csf = createSessionFactory(locator);
           ClientSession session = csf.createSession()) {

         ClientConsumer consumer1_q1 = session.createConsumer(queueName1);

         // add another consumer (on separate session)
         ClientSession session_two = csf.createSession();
         ClientConsumer consumer2_q1 = session_two.createConsumer(queueName1);

         //first(normal) invocation - ensure 2 consumers returned
         //used to block thread, until the delay() has been called.
         delayCalled = new CountDownLatch(1);

         String consumersAsJsonString = serverControl.listConsumers(createJsonFilter("", "", ""), 1, 10);

         JsonObject consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         JsonArray consumersArray = (JsonArray) consumersAsJsonObject.get("data");

         Assert.assertEquals("number of  consumers returned from query", 2, consumersArray.size());
         Assert.assertEquals("check consumer's queue", queueName1.toString(), consumersArray.getJsonObject(0).getString("queueName"));
         Assert.assertNotEquals("check session", "", consumersArray.getJsonObject(0).getString("sessionName"));

         //second invocation - close session during listConsumers()

         //used to block thread, until the delay() has been called.
         delayCalled = new CountDownLatch(1);

         executorService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  //wait until the delay occurs and close the session.
                  delayCalled.await();
                  session.close();
               } catch (Exception e) {
                  e.printStackTrace();
               }

            }
         });

         consumersAsJsonString = serverControl.listConsumers(createJsonFilter("", "", ""), 1, 10);

         consumersAsJsonObject = JsonUtil.readJsonObject(consumersAsJsonString);
         consumersArray = (JsonArray) consumersAsJsonObject.get("data");

         // session is closed before Json string is created - should only be one consumer returned
         Assert.assertEquals("number of  consumers returned from query", 1, consumersArray.size());
         Assert.assertEquals("check consumer's queue", queueName1.toString(), consumersArray.getJsonObject(0).getString("queueName"));
         Assert.assertNotEquals("check session", "", consumersArray.getJsonObject(0).getString("sessionName"));

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