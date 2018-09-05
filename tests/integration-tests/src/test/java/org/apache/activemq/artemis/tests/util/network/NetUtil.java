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

package org.apache.activemq.artemis.tests.util.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class NetUtil {

   public static boolean checkIP(String ip) throws Exception {
      InetAddress ipAddress = null;
      try {
         ipAddress = InetAddress.getByName(ip);
      } catch (Exception e) {
         e.printStackTrace(); // not supposed to happen
         return false;
      }
      NetworkHealthCheck healthCheck = new NetworkHealthCheck(null, 100, 100);
      return healthCheck.check(ipAddress);
   }

   public static AtomicInteger nextDevice = new AtomicInteger(0);

   // IP / device (device being only valid on linux)
   public static Map<String, String> networks = new ConcurrentHashMap<>();

   private enum OS {
      MAC, LINUX, NON_SUPORTED;
   }

   static final OS osUsed;
   static final String user = System.getProperty("user.name");

   static {
      OS osTmp;

      String propOS = System.getProperty("os.name").toUpperCase();

      if (propOS.contains("MAC")) {
         osTmp = OS.MAC;
      } else if (propOS.contains("LINUX")) {
         osTmp = OS.LINUX;
      } else {
         osTmp = OS.NON_SUPORTED;
      }

      osUsed = osTmp;
   }

   public static void assumeSudo() {
      Assume.assumeTrue("non supported OS", osUsed != OS.NON_SUPORTED);
      if (!canSudo()) {
         System.out.println("Add the following at the end of your /etc/sudoers (use the visudo command)");
         System.out.println("# ------------------------------------------------------- ");
         System.out.println(user + " ALL = NOPASSWD: /sbin/ifconfig");
         System.out.println("# ------------------------------------------------------- ");
         Assume.assumeFalse(true);
      }
   }

   public static void cleanup() {
      nextDevice.set(0);

      Set entrySet = networks.entrySet();
      Iterator iter = entrySet.iterator();
      while (iter.hasNext()) {
         Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
         try {
            netDown(entry.getKey(), entry.getValue());
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   public static void netUp(String ip) throws Exception {
      String deviceID = "lo:" + nextDevice.incrementAndGet();
      if (osUsed == OS.MAC) {
         if (runCommand("sudo", "-n", "ifconfig", "lo0", "alias", ip) != 0) {
            Assert.fail("Cannot sudo ifconfig for ip " + ip);
         }
         networks.put(ip, "lo0");
      } else if (osUsed == OS.LINUX) {
         if (runCommand("sudo", "-n", "ifconfig", deviceID, ip, "netmask", "255.0.0.0") != 0) {
            Assert.fail("Cannot sudo ifconfig for ip " + ip);
         }
         networks.put(ip, deviceID);
      } else {
         Assert.fail("OS not supported");
      }
   }

   public static void netDown(String ip) throws Exception {
      String device = networks.remove(ip);
      Assert.assertNotNull("ip " + ip + "wasn't set up before", device);
      netDown(ip, device);

   }

   private static void netDown(String ip, String device) throws Exception {
      if (osUsed == OS.MAC) {
         if (runCommand("sudo", "-n", "ifconfig", "lo0", "-alias", ip) != 0) {
            Assert.fail("Cannot sudo ifconfig for ip " + ip);
         }
      } else if (osUsed == OS.LINUX) {
         if (runCommand("sudo", "-n", "ifconfig", device, "down") != 0) {
            Assert.fail("Cannot sudo ifconfig for ip " + ip);
         }
      } else {
         Assert.fail("OS not supported");
      }
   }

   private static final Logger logger = Logger.getLogger(NetUtil.class);

   public static int runCommand(String... command) throws Exception {
      return runCommand(10, TimeUnit.SECONDS, command);
   }

   public static int runCommand(long timeout, TimeUnit timeoutUnit, String... command) throws Exception {

      logCommand(command);

      // it did not work with a simple isReachable, it could be because there's no root access, so we will try ping executable
      ProcessBuilder processBuilder = new ProcessBuilder(command);
      final Process process = processBuilder.start();

      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               readStream(process.getInputStream(), true);
            } catch (Exception dontCare) {

            }
         }
      };
      Thread t2 = new Thread() {
         @Override
         public void run() {
            try {
               readStream(process.getErrorStream(), true);
            } catch (Exception dontCare) {

            }
         }
      };
      t2.start();

      int value = process.waitFor();

      t.join(timeoutUnit.toMillis(timeout));
      Assert.assertFalse(t.isAlive());
      t2.join(timeoutUnit.toMillis(timeout));

      return value;
   }

   private static void logCommand(String[] command) {
      StringBuffer logCommand = new StringBuffer();
      for (String c : command) {
         logCommand.append(c + " ");
      }
      System.out.println("NetUTIL command::" + logCommand.toString());
   }

   public static boolean canSudo() {
      try {
         return runCommand("sudo", "-n", "ifconfig") == 0;
      } catch (Exception e) {
         e.printStackTrace();
         return false;
      }
   }

   @Test
   public void testCanSudo() throws Exception {
      Assert.assertTrue(canSudo());
   }

   private static void readStream(InputStream stream, boolean error) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

      String inputLine;
      while ((inputLine = reader.readLine()) != null) {
         if (error) {
            logger.warn(inputLine);
         } else {
            logger.trace(inputLine);
         }
      }

      reader.close();
   }

}
