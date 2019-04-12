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

package org.apache.activemq.artemis.tests.integration.jmx;

import com.sun.jmx.remote.internal.ProxyRef;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.rmi.server.UnicastRef;
import sun.rmi.transport.LiveRef;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnection;
import javax.management.remote.rmi.RMIConnector;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteRef;
import java.util.concurrent.TimeUnit;

/**
 * This test checks JMX connection to Artemis with both necessary ports set up so that it's easier to tunnel through
 * firewalls.
 */
public class JmxConnectionTest extends ActiveMQTestBase {

   private static final String MANAGEMENT_XML = "/jmx-test-management.xml";

   // Make sure these values are always the same as in the MANAGEMENT_XML configuration file
   private static final String JMX_SERVER_HOSTNAME = "127.0.0.1";
   private static final int JMX_SERVER_PORT = 10099;
   private static final int RMI_REGISTRY_PORT = 10098;

   @Override
   @Before
   public void setUp() throws Exception {

      super.setUp();

      /* This needs to be disabled because otherwise there would be a lot of complains about Artemis' own running threads
       * and I suppose that this kind of leaks is most likely tested somewhere else.
       */
      disableCheckThread();

      // Artemis instance dir
      File instanceDir = new File(temporaryFolder.getRoot(), "instance");

      // Create new Artemis instance
      Run.setEmbedded(true);
      Artemis.main("create", instanceDir.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune",
              "--no-web", "--no-amqp-acceptor", "--no-mqtt-acceptor", "--no-stomp-acceptor", "--no-hornetq-acceptor");

      // Configure (this is THE subject of testing)
      File managementConfigFile = new File(instanceDir, "etc/management.xml");
      Files.copy(getClass().getResourceAsStream(MANAGEMENT_XML), managementConfigFile.toPath(),
              StandardCopyOption.REPLACE_EXISTING);

      // Point the server to the instance directory
      System.setProperty("artemis.instance", instanceDir.getAbsolutePath());

      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", JMX_SERVER_HOSTNAME);

      // Enable guest login module (dunno why this isn't automatic)
      System.setProperty("java.security.auth.login.config", instanceDir.getAbsolutePath() + "/etc/login.config");

      // Run Artemis server
      Artemis.internalExecute("run");
   }

   @Test
   public void testJmxConnection() throws Exception {

      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector;

      try {
         jmxConnector = JMXConnectorFactory.connect(url);
         logAndSystemOut("Successfully connected to: " + urlString);
      } catch (Exception e) {
         logAndSystemOut("JMX connection failed: " + urlString, e);
         Assert.fail(e.getMessage());
         return;
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

         // 6. LiveRef::getPort is expected to be the same as the RMI registry port configured via management.xml
         /* Accidentally, it can happen that even with the RMI registry port unconfigured the randomly selected port
          * will be the same as expected by the test which will make it succeed. But it's highly unlikely.
          */
         Assert.assertEquals(RMI_REGISTRY_PORT, liveRef.getPort());

      } finally {
         jmxConnector.close();
      }
   }

   @After
   @Override
   public void tearDown() throws Exception {
      Artemis.internalExecute("stop");
      Run.latchRunning.await(5, TimeUnit.SECONDS);
      super.tearDown();
   }

}
