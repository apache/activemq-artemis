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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.junit.Before;

public class DivertControlUsingCoreTest extends DivertControlTest {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientSession session;
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlTest overrides --------------------------------

   @Override
   protected DivertControl createManagementControl(final String name) throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);
      session.start();

      return new DivertControl() {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session, ResourceNames.CORE_DIVERT + name);

         public String getAddress() {
            return (String) proxy.retrieveAttributeValue("address");
         }

         public String getFilter() {
            return (String) proxy.retrieveAttributeValue("filter");
         }

         public String getForwardingAddress() {
            return (String) proxy.retrieveAttributeValue("forwardingAddress");
         }

         public String getRoutingName() {
            return (String) proxy.retrieveAttributeValue("routingName");
         }

         public String getTransformerClassName() {
            return (String) proxy.retrieveAttributeValue("transformerClassName");
         }

         public String getUniqueName() {
            return (String) proxy.retrieveAttributeValue("uniqueName");
         }

         public boolean isExclusive() {
            return (Boolean) proxy.retrieveAttributeValue("exclusive");
         }

      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createInVMNonHALocator();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
