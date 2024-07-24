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
package org.apache.activemq.artemis.core.server.management;

import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.tests.util.ServerTestBase;
import org.junit.Test;

import javax.management.ObjectName;
import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArtemisMBeanServerGuardTest extends ServerTestBase {
   @Test
   public void testInvokeNoMethod() throws Throwable {
      ArtemisMBeanServerGuard  guard = new ArtemisMBeanServerGuard();
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("testdomain", "myBroker");
      ObjectName activeMQServerObjectName = objectNameBuilder.getActiveMQServerObjectName();
      assertTrue(guard.canInvoke(activeMQServerObjectName.getCanonicalName(), null));
   }

   @Test
   public void testCantInvokeMethod() throws Throwable {
      ArtemisMBeanServerGuard  guard = new ArtemisMBeanServerGuard();
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("testdomain", "myBroker");
      ObjectName activeMQServerObjectName = objectNameBuilder.getActiveMQServerObjectName();
      assertFalse(guard.canInvoke(activeMQServerObjectName.getCanonicalName(), "getSomething"));
   }


   @Test
   public void testCanInvokeMethodWhiteList() throws Throwable {
      ArtemisMBeanServerGuard  guard = new ArtemisMBeanServerGuard();
      JMXAccessControlList controlList = new JMXAccessControlList();
      guard.setJMXAccessControlList(controlList);
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("testdomain", "myBroker");
      ObjectName activeMQServerObjectName = objectNameBuilder.getActiveMQServerObjectName();
      controlList.addToAllowList("testdomain", "broker=myBroker");
      assertTrue(guard.canInvoke(activeMQServerObjectName.getCanonicalName(), "getSomething"));
   }


   @Test
   public void testCanInvokeMethodHasRole() throws Throwable {
      ArtemisMBeanServerGuard  guard = new ArtemisMBeanServerGuard();
      JMXAccessControlList controlList = new JMXAccessControlList();
      guard.setJMXAccessControlList(controlList);
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("testdomain", "myBroker");
      ObjectName activeMQServerObjectName = objectNameBuilder.getActiveMQServerObjectName();
      controlList.addToRoleAccess("testdomain", "broker=myBroker", "getSomething", "admin");
      Subject subject = new Subject();
      subject.getPrincipals().add(new RolePrincipal("admin"));
      Object result = Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
         try {
            return guard.canInvoke(activeMQServerObjectName.getCanonicalName(), "getSomething");
         } catch (Exception e1) {
            return e1;
         }
      });
      assertTrue((Boolean) result);
   }


   @Test
   public void testCanInvokeMethodDoeNotHasRole() throws Throwable {
      ArtemisMBeanServerGuard  guard = new ArtemisMBeanServerGuard();
      JMXAccessControlList controlList = new JMXAccessControlList();
      guard.setJMXAccessControlList(controlList);
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("testdomain", "myBroker");
      ObjectName activeMQServerObjectName = objectNameBuilder.getActiveMQServerObjectName();
      controlList.addToRoleAccess("testdomain", "broker=myBroker", "getSomething", "admin");
      Subject subject = new Subject();
      subject.getPrincipals().add(new RolePrincipal("view"));
      Object result = Subject.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
         try {
            return guard.canInvoke(activeMQServerObjectName.getCanonicalName(), "getSomething");
         } catch (Exception e1) {
            return e1;
         }
      });
      assertFalse((Boolean) result);
   }
}
