/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.jmx;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnection;
import javax.management.remote.rmi.RMIConnector;
import java.lang.reflect.Field;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteRef;

import com.sun.jmx.remote.internal.ProxyRef;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.rmi.server.UnicastRef;
import sun.rmi.transport.LiveRef;

/**
 * This test checks JMX connection to Artemis with both necessary ports set up so that it's easier to tunnel through
 * firewalls.
 */
public class JmxConnectionTest extends SmokeTestBase {

   // This test will use a smoke created by the pom on this project (smoke-tsts)

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 10099;
   private static final int RMI_REGISTRY_PORT = 10098;

   public static final String SERVER_NAME_0 = "jmx";

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
   }

   @Test
   public void testJmxConnection() throws Throwable {
      try {

         // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
         System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

         // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
         String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT + "/jmxrmi";

         JMXServiceURL url = new JMXServiceURL(urlString);
         JMXConnector jmxConnector;

         try {
            jmxConnector = JMXConnectorFactory.connect(url);
            System.out.println("Successfully connected to: " + urlString);
         } catch (Exception e) {
            jmxConnector = null;
            e.printStackTrace();
            Assert.fail(e.getMessage());
         }

         try {

         /* Now I need to extract the RMI registry port to make sure it's equal to the configured one. It's gonna be
          * messy because I have to use reflection to reach the information.
          */

            Assert.assertTrue(jmxConnector instanceof RMIConnector);

            // 1. RMIConnector::connection is expected to be RMIConnectionImpl_Stub
            Field connectionField = RMIConnector.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            RMIConnection rmiConnection = (RMIConnection) connectionField.get(jmxConnector);

            // 2. RMIConnectionImpl_Stub extends RemoteStub which extends RemoteObject
            Assert.assertTrue(rmiConnection instanceof RemoteObject);
            RemoteObject remoteObject = (RemoteObject) rmiConnection;

            // 3. RemoteObject::getRef is hereby expected to return ProxyRef
            RemoteRef remoteRef = remoteObject.getRef();
            Assert.assertTrue(remoteRef instanceof ProxyRef);
            ProxyRef proxyRef = (ProxyRef) remoteRef;

            // 4. ProxyRef::ref is expected to contain UnicastRef (UnicastRef2 resp.)
            Field refField = ProxyRef.class.getDeclaredField("ref");
            refField.setAccessible(true);
            remoteRef = (RemoteRef) refField.get(proxyRef);
            Assert.assertTrue(remoteRef instanceof UnicastRef);

            // 5. UnicastRef::getLiveRef returns LiveRef
            LiveRef liveRef = ((UnicastRef) remoteRef).getLiveRef();

            Assert.assertEquals(RMI_REGISTRY_PORT, liveRef.getPort());

         } finally {
            jmxConnector.close();
         }
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      }
   }


}
