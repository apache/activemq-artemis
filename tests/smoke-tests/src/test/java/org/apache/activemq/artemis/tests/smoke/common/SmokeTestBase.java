/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.common;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.cli.commands.Stop;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.After;
import org.junit.Assert;

public class SmokeTestBase extends ActiveMQTestBase {
   Set<Process> processes = new HashSet<>();

   public static final String basedir = System.getProperty("basedir");

   @After
   public void after() throws Exception {
      for (Process process : processes) {
         try {
            ServerUtil.killServer(process, true);
         } catch (Throwable e) {
            e.printStackTrace();
         }
      }
      processes.clear();
   }

   public void killServer(Process process) {
      processes.remove(process);
      try {
         ServerUtil.killServer(process);
      } catch (Throwable e) {
         e.printStackTrace();
      }
   }

   protected static void stopServerWithFile(String serverLocation) throws IOException {
      File serverPlace = new File(serverLocation);
      File etcPlace = new File(serverPlace, "etc");
      File stopMe = new File(etcPlace, Stop.STOP_FILE_NAME);
      Assert.assertTrue(stopMe.createNewFile());
   }

   public static String getServerLocation(String serverName) {
      return basedir + "/target/" + serverName;
   }

   public static void cleanupData(String serverName) {
      String location = getServerLocation(serverName);
      deleteDirectory(new File(location, "data"));
   }

   public void addProcess(Process process) {
      processes.add(process);
   }

   public Process startServer(String serverName, int portID, int timeout) throws Exception {
      Process process = ServerUtil.startServer(getServerLocation(serverName), serverName, portID, timeout);
      addProcess(process);
      return process;
   }

   public Process startServer(String serverName, String uri, int timeout) throws Exception {
      Process process = ServerUtil.startServer(getServerLocation(serverName), serverName, uri, timeout);
      addProcess(process);
      return process;
   }

   protected static JMXConnector getJmxConnector(String hostname, int port) throws Exception {
      // Without this, the RMI server would bind to the default interface IP (the user's local IP mostly)
      System.setProperty("java.rmi.server.hostname", hostname);

      // I don't specify both ports here manually on purpose. See actual RMI registry connection port extraction below.
      String urlString = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";

      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector jmxConnector = null;

      try {
         jmxConnector = JMXConnectorFactory.connect(url);
         System.out.println("Successfully connected to: " + urlString);
      } catch (Exception e) {
         jmxConnector = null;
         e.printStackTrace();
         Assert.fail(e.getMessage());
      }
      return jmxConnector;
   }
}
