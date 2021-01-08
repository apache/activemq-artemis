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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AddressSettingsTest extends ActiveMQTestBase {

   private final SimpleString addressA = new SimpleString("addressA");

   private final SimpleString addressA2 = new SimpleString("add.addressA");

   private final SimpleString addressB = new SimpleString("addressB");

   private final SimpleString addressB2 = new SimpleString("add.addressB");

   private final SimpleString addressC = new SimpleString("addressC");

   private final SimpleString queueA = new SimpleString("queueA");

   private final SimpleString queueB = new SimpleString("queueB");

   private final SimpleString queueC = new SimpleString("queueC");

   private final SimpleString dlaA = new SimpleString("dlaA");

   private final SimpleString dlqA = new SimpleString("dlqA");

   private final SimpleString dlaB = new SimpleString("dlaB");

   private final SimpleString dlqB = new SimpleString("dlqB");

   private final SimpleString dlaC = new SimpleString("dlaC");

   private final SimpleString dlqC = new SimpleString("dlqC");

   @Test
   public void testSimpleHierarchyWithDLA() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();
      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(dlaA).setMaxDeliveryAttempts(1);
      AddressSettings addressSettings2 = new AddressSettings().setDeadLetterAddress(dlaB).setMaxDeliveryAttempts(1);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(addressA.toString(), addressSettings);
      repos.addMatch(addressB.toString(), addressSettings2);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, false);
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setDurable(false));
      session.createQueue(new QueueConfiguration(queueB).setAddress(addressB).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqA).setAddress(dlaA).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqB).setAddress(dlaB).setDurable(false));
      ClientSession sendSession = sf.createSession(false, true, true);
      ClientMessage cm = sendSession.createMessage(true);
      cm.getBodyBuffer().writeString("A");
      ClientMessage cm2 = sendSession.createMessage(true);
      cm2.getBodyBuffer().writeString("B");
      ClientProducer cp1 = sendSession.createProducer(addressA);
      ClientProducer cp2 = sendSession.createProducer(addressB);
      cp1.send(cm);
      cp2.send(cm2);

      ClientConsumer dlqARec = session.createConsumer(dlqA);
      ClientConsumer dlqBrec = session.createConsumer(dlqB);
      ClientConsumer cc1 = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      session.start();
      ClientMessage message = cc1.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc2.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      session.rollback();
      cc1.close();
      cc2.close();
      message = dlqARec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("A", message.getBodyBuffer().readString());
      message = dlqBrec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("B", message.getBodyBuffer().readString());
      sendSession.close();
      session.close();

   }

   @Test
   public void test2LevelHierarchyWithDLA() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();
      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(dlaA).setMaxDeliveryAttempts(1);
      AddressSettings addressSettings2 = new AddressSettings().setDeadLetterAddress(dlaB).setMaxDeliveryAttempts(1);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(addressA.toString(), addressSettings);
      repos.addMatch("#", addressSettings2);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, false);
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setDurable(false));
      session.createQueue(new QueueConfiguration(queueB).setAddress(addressB).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqA).setAddress(dlaA).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqB).setAddress(dlaB).setDurable(false));
      ClientSession sendSession = sf.createSession(false, true, true);
      ClientMessage cm = sendSession.createMessage(true);
      cm.getBodyBuffer().writeString("A");
      ClientMessage cm2 = sendSession.createMessage(true);
      cm2.getBodyBuffer().writeString("B");
      ClientProducer cp1 = sendSession.createProducer(addressA);
      ClientProducer cp2 = sendSession.createProducer(addressB);
      cp1.send(cm);
      cp2.send(cm2);

      ClientConsumer dlqARec = session.createConsumer(dlqA);
      ClientConsumer dlqBrec = session.createConsumer(dlqB);
      ClientConsumer cc1 = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      session.start();
      ClientMessage message = cc1.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc2.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      session.rollback();
      cc1.close();
      cc2.close();
      message = dlqARec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("A", message.getBodyBuffer().readString());
      message = dlqBrec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("B", message.getBodyBuffer().readString());
      sendSession.close();
      session.close();

   }

   @Test
   public void test2LevelWordHierarchyWithDLA() throws Exception {
      ActiveMQServer server = createServer(false);
      server.start();
      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(dlaA).setMaxDeliveryAttempts(1);
      AddressSettings addressSettings2 = new AddressSettings().setDeadLetterAddress(dlaB).setMaxDeliveryAttempts(1);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(addressA.toString(), addressSettings);
      repos.addMatch("*", addressSettings2);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, false);
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA).setDurable(false));
      session.createQueue(new QueueConfiguration(queueB).setAddress(addressB).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqA).setAddress(dlaA).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqB).setAddress(dlaB).setDurable(false));
      ClientSession sendSession = sf.createSession(false, true, true);
      ClientMessage cm = sendSession.createMessage(true);
      cm.getBodyBuffer().writeString("A");
      ClientMessage cm2 = sendSession.createMessage(true);
      cm2.getBodyBuffer().writeString("B");
      ClientProducer cp1 = sendSession.createProducer(addressA);
      ClientProducer cp2 = sendSession.createProducer(addressB);
      cp1.send(cm);
      cp2.send(cm2);

      ClientConsumer dlqARec = session.createConsumer(dlqA);
      ClientConsumer dlqBrec = session.createConsumer(dlqB);
      ClientConsumer cc1 = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      session.start();
      ClientMessage message = cc1.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc2.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      session.rollback();
      cc1.close();
      cc2.close();
      message = dlqARec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("A", message.getBodyBuffer().readString());
      message = dlqBrec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("B", message.getBodyBuffer().readString());
      sendSession.close();
      session.close();
   }

   @Test
   public void test3LevelHierarchyWithDLA() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();
      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(dlaA).setMaxDeliveryAttempts(1);
      AddressSettings addressSettings2 = new AddressSettings().setDeadLetterAddress(dlaB).setMaxDeliveryAttempts(1);
      AddressSettings addressSettings3 = new AddressSettings().setDeadLetterAddress(dlaC).setMaxDeliveryAttempts(1);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(addressA2.toString(), addressSettings);
      repos.addMatch("add.*", addressSettings2);
      repos.addMatch("#", addressSettings3);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, false);
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA2).setDurable(false));
      session.createQueue(new QueueConfiguration(queueB).setAddress(addressB2).setDurable(false));
      session.createQueue(new QueueConfiguration(queueC).setAddress(addressC).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqA).setAddress(dlaA).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqB).setAddress(dlaB).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqC).setAddress(dlaC).setDurable(false));
      ClientSession sendSession = sf.createSession(false, true, true);
      ClientMessage cm = sendSession.createMessage(true);
      cm.getBodyBuffer().writeString("A");
      ClientMessage cm2 = sendSession.createMessage(true);
      cm2.getBodyBuffer().writeString("B");
      ClientMessage cm3 = sendSession.createMessage(true);
      cm3.getBodyBuffer().writeString("C");
      ClientProducer cp1 = sendSession.createProducer(addressA2);
      ClientProducer cp2 = sendSession.createProducer(addressB2);
      ClientProducer cp3 = sendSession.createProducer(addressC);
      cp1.send(cm);
      cp2.send(cm2);
      cp3.send(cm3);

      ClientConsumer dlqARec = session.createConsumer(dlqA);
      ClientConsumer dlqBrec = session.createConsumer(dlqB);
      ClientConsumer dlqCrec = session.createConsumer(dlqC);
      ClientConsumer cc1 = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      ClientConsumer cc3 = session.createConsumer(queueC);
      session.start();
      ClientMessage message = cc1.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc2.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc3.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      session.rollback();
      cc1.close();
      cc2.close();
      cc3.close();
      message = dlqARec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("A", message.getBodyBuffer().readString());
      message = dlqBrec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("B", message.getBodyBuffer().readString());
      message = dlqCrec.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals("C", message.getBodyBuffer().readString());
      sendSession.close();
      session.close();

   }

   @Test
   public void test3LevelHierarchyPageSizeBytes() throws Exception {
      ActiveMQServer server = createServer(true);
      server.start();

      AddressSettings level1 = new AddressSettings().setPageSizeBytes(100 * 1024);
      AddressSettings level2 = new AddressSettings();
      AddressSettings level3 = new AddressSettings();
      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().setDefault(null);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch("test.foo.bar", level3);
      repos.addMatch("test.foo.#", level2);
      repos.addMatch("test.#", level1);

      assertEquals(100 * 1024, server.getAddressSettingsRepository().getMatch("test.foo.bar").getPageSizeBytes());
   }

   @Test
   public void testOverrideHierarchyWithDLA() throws Exception {
      ActiveMQServer server = createServer(false);

      server.start();
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1);
      AddressSettings addressSettings2 = new AddressSettings().setMaxDeliveryAttempts(1);
      AddressSettings addressSettings3 = new AddressSettings().setDeadLetterAddress(dlaC).setMaxDeliveryAttempts(1);
      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(addressA2.toString(), addressSettings);
      repos.addMatch("add.*", addressSettings2);
      repos.addMatch("#", addressSettings3);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, false);
      session.createQueue(new QueueConfiguration(queueA).setAddress(addressA2).setDurable(false));
      session.createQueue(new QueueConfiguration(queueB).setAddress(addressB2).setDurable(false));
      session.createQueue(new QueueConfiguration(queueC).setAddress(addressC).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqA).setAddress(dlaA).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqB).setAddress(dlaB).setDurable(false));
      session.createQueue(new QueueConfiguration(dlqC).setAddress(dlaC).setDurable(false));
      ClientSession sendSession = sf.createSession(false, true, true);
      ClientMessage cm = sendSession.createMessage(true);
      ClientMessage cm2 = sendSession.createMessage(true);
      ClientMessage cm3 = sendSession.createMessage(true);
      ClientProducer cp1 = sendSession.createProducer(addressA2);
      ClientProducer cp2 = sendSession.createProducer(addressB2);
      ClientProducer cp3 = sendSession.createProducer(addressC);
      cp1.send(cm);
      cp2.send(cm2);
      cp3.send(cm3);

      ClientConsumer dlqCrec = session.createConsumer(dlqC);
      ClientConsumer cc1 = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueB);
      ClientConsumer cc3 = session.createConsumer(queueC);
      session.start();
      ClientMessage message = cc1.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc2.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      message = cc3.receive(5000);
      Assert.assertNotNull(message);
      message.acknowledge();
      session.rollback();
      cc1.close();
      cc2.close();
      cc3.close();
      message = dlqCrec.receive(5000);
      Assert.assertNotNull(message);
      message = dlqCrec.receive(5000);
      Assert.assertNotNull(message);
      message = dlqCrec.receive(5000);
      Assert.assertNotNull(message);
      sendSession.close();
      session.close();

   }
}
