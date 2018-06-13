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

import org.junit.Test;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

public class JmxAccessControlTest extends JmxSecurityTestBase {

   @Test
   public void testJmxRoleBasedAccess() throws Exception {
      JMXConnector goodConnector = connectJmxWithAuthen("jmxgooduser", "jmxgoodpassword", false);

      try {
         invokeServerOperation(goodConnector);
      } finally {
         goodConnector.close();
      }

      JMXConnector badConnector = connectJmxWithAuthen("jmxuser", "jmxuserpassword", false);
      try {
         invokeServerOperation(badConnector);
         fail("The role shouldn't be able to access the operation");
      } catch (SecurityException e) {
         //ok
      } finally {
         badConnector.close();
      }
   }

   private void invokeServerOperation(JMXConnector connector) throws Exception {
      MBeanServerConnection serverConn = connector.getMBeanServerConnection();
      serverConn.invoke(new ObjectName("org.apache.activemq.artemis:broker=\"amq\""), "listConnectionIDs", null, null);
   }

   @Test
   public void testJmxConnectionAuthen() throws Exception {
      connectJmxWithAuthen("jmxgooduser", "jmxgoodpassword");
      connectJmxWithAuthen("jmxuser", "jmxuserpassword");
      connectJmxWithAuthen("jmxuser1", "jmxuserpassword1");
      connectJmxWithAuthen("jmxuser2", "jmxuserpassword2");

      if (!isUseJaas()) {
         return;
      }

      //wrong pass
      try {
         connectJmxWithAuthen("jmxuser", "wrongpassword");
         fail("didn't get exception with wrong password");
      } catch (SecurityException e) {
         //correct
      }
      //non exist user
      try {
         connectJmxWithAuthen("nouser", "jmxuserpassword");
         fail("didn't get exception with wrong principal");
      } catch (SecurityException e) {
         //correct
      }
   }
}
