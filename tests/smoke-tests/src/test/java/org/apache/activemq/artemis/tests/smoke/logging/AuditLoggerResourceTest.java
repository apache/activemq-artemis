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
package org.apache.activemq.artemis.tests.smoke.logging;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import java.util.HashMap;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.junit.Test;

public class AuditLoggerResourceTest extends AuditLoggerTestBase {

   @Override
   protected String getServerName() {
      return "audit-logging";
   }

   @Test
   public void testAuditResourceLog() throws Exception {
      JMXConnector jmxConnector = getJmxConnector();

      try {
         MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
         String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
         ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
         ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

         serverControl.createAddress("auditAddress", "ANYCAST,MULTICAST");
         checkAuditLogRecord(true, "successfully created address:");
         serverControl.updateAddress("auditAddress", "ANYCAST");
         checkAuditLogRecord(true, "successfully updated address:");
         serverControl.deleteAddress("auditAddress");
         checkAuditLogRecord(true, "successfully deleted address:");
         serverControl.createQueue("auditAddress", "auditQueue", "ANYCAST");
         checkAuditLogRecord(true, "successfully created queue:");
         serverControl.updateQueue("auditQueue", "ANYCAST", -1, false);
         final QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection,
               objectNameBuilder.getQueueObjectName(new SimpleString( "auditAddress"), new SimpleString("auditQueue"), RoutingType.ANYCAST),
               QueueControl.class,
               false);
         checkAuditLogRecord(true, "successfully updated queue:");
         queueControl.removeAllMessages();
         checkAuditLogRecord(true, "has removed 0 messages");
         queueControl.sendMessage(new HashMap<>(), 0, "foo", true, "admin", "admin");
         checkAuditLogRecord(true, "sent message to");
         CompositeData[] browse = queueControl.browse();
         checkAuditLogRecord(true, "browsed " + browse.length + " messages");
         serverControl.destroyQueue("auditQueue");
         checkAuditLogRecord(true, "successfully deleted queue:");

         ServerLocator locator = createNettyNonHALocator();
         ClientSessionFactory sessionFactory = locator.createSessionFactory();
         ClientSession session = sessionFactory.createSession("admin", "admin", false, false, true, false, 0);
         ClientProducer producer = session.createProducer("myQ");
         producer.send(session.createMessage(true));
         locator.close();

      } finally {
         jmxConnector.close();
      }
   }
}
