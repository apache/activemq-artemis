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

package org.apache.activemq.artemis.core.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLConnection;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.logs.ActiveMQUtilLogger;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.jboss.logging.Logger;

/**
 * This will use {@link InetAddress#isReachable(int)} to determine if the network is alive.
 * It will have a set of addresses, and if any address is reached the network will be considered alive.
 */
public class NetworkHealthCheck extends ActiveMQScheduledComponent {

   private static final Logger logger = Logger.getLogger(NetworkHealthCheck.class);

   private final Set<ActiveMQComponent> componentList = new ConcurrentHashSet<>();
   private final Set<InetAddress> addresses = new ConcurrentHashSet<>();
   private final Set<URL> urls = new ConcurrentHashSet<>();
   private NetworkInterface networkInterface;

   public static final String IPV6_DEFAULT_COMMAND = "ping6 -c 1 %2$s";

   public static final String IPV4_DEFAULT_COMMAND = "ping -c 1 -t %d %s";

   private String ipv4Command = IPV4_DEFAULT_COMMAND;

   private String ipv6Command = IPV6_DEFAULT_COMMAND;

   // To be used on tests. As we use the loopback as a valid address on tests.
   private boolean ignoreLoopback = false;

   /**
    * The timeout to be used on isReachable
    */
   private int networkTimeout;

   public NetworkHealthCheck() {
      this(null, 1000, 1000);
   }

   public NetworkHealthCheck(String nicName, long checkPeriod, int networkTimeout) {
      super(null, null, checkPeriod, TimeUnit.MILLISECONDS, false);
      this.networkTimeout = networkTimeout;
      this.setNICName(nicName);
   }

   public NetworkHealthCheck setNICName(String nicName) {
      NetworkInterface netToUse;
      try {
         if (nicName != null) {
            netToUse = NetworkInterface.getByName(nicName);
         } else {
            netToUse = null;
         }
      } catch (Exception e) {
         ActiveMQUtilLogger.LOGGER.failedToSetNIC(e, nicName == null ? " " : nicName);
         netToUse = null;
      }

      this.networkInterface = netToUse;
      return this;
   }

   public boolean isIgnoreLoopback() {
      return ignoreLoopback;
   }

   public NetworkHealthCheck setIgnoreLoopback(boolean ignoreLoopback) {
      this.ignoreLoopback = ignoreLoopback;
      return this;
   }

   public Set<InetAddress> getAddresses() {
      return addresses;
   }

   public Set<URL> getUrls() {
      return urls;
   }

   public String getNICName() {
      if (networkInterface != null) {
         return networkInterface.getName();
      } else {
         return null;
      }
   }

   public NetworkHealthCheck parseAddressList(String addressList) {
      if (addressList != null) {
         String[] addresses = addressList.split(",");

         for (String address : addresses) {
            if (!address.trim().isEmpty()) {
               try {
                  this.addAddress(InetAddress.getByName(address.trim()));
               } catch (Exception e) {
                  ActiveMQUtilLogger.LOGGER.failedToParseAddressList(e, addressList);
               }
            }
         }
      }

      return this;
   }

   public NetworkHealthCheck parseURIList(String addressList) {
      if (addressList != null) {
         String[] addresses = addressList.split(",");

         for (String address : addresses) {
            if (!address.trim().isEmpty()) {
               try {
                  this.addURL(new URL(address.trim()));
               } catch (Exception e) {
                  ActiveMQUtilLogger.LOGGER.failedToParseUrlList(e, addressList);
               }
            }
         }
      }

      return this;
   }

   @Override
   protected ActiveMQThreadFactory getThreadFactory() {
      return new ActiveMQThreadFactory("NetworkChecker", "Network-Checker-", false, getThisClassLoader());
   }


   private ClassLoader getThisClassLoader() {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
         @Override
         public ClassLoader run() {
            return NetworkHealthCheck.this.getClass().getClassLoader();
         }
      });

   }

   public int getNetworkTimeout() {
      return networkTimeout;
   }

   @Override
   public NetworkHealthCheck setPeriod(long period) {
      super.setPeriod(period);
      return this;
   }

   @Override
   public NetworkHealthCheck setTimeUnit(TimeUnit timeUnit) {
      super.setTimeUnit(timeUnit);
      return this;
   }

   public NetworkHealthCheck setNetworkTimeout(int networkTimeout) {
      this.networkTimeout = networkTimeout;
      return this;
   }

   public NetworkHealthCheck addComponent(ActiveMQComponent component) {
      componentList.add(component);
      checkStart();
      return this;
   }

   public NetworkHealthCheck clearComponents() {
      componentList.clear();
      return this;
   }

   public NetworkHealthCheck addAddress(InetAddress address) {
      if (!check(address)) {
         ActiveMQUtilLogger.LOGGER.addressWasntReacheable(address.toString());
      }

      if (!ignoreLoopback && address.isLoopbackAddress()) {
         ActiveMQUtilLogger.LOGGER.addressloopback(address.toString());
      } else {
         addresses.add(address);
         checkStart();
      }
      return this;
   }

   public NetworkHealthCheck removeAddress(InetAddress address) {
      addresses.remove(address);
      return this;
   }

   public NetworkHealthCheck clearAddresses() {
      addresses.clear();
      return this;
   }

   public NetworkHealthCheck addURL(URL url) {
      if (!check(url)) {
         ActiveMQUtilLogger.LOGGER.urlWasntReacheable(url.toString());
      }
      urls.add(url);
      checkStart();
      return this;
   }

   public NetworkHealthCheck removeURL(URL url) {
      urls.remove(url);
      return this;
   }

   public NetworkHealthCheck clearURL() {
      urls.clear();
      return this;
   }

   public String getIpv4Command() {
      return ipv4Command;
   }

   public NetworkHealthCheck setIpv4Command(String ipv4Command) {
      this.ipv4Command = ipv4Command;
      return this;
   }

   public String getIpv6Command() {
      return ipv6Command;
   }

   public NetworkHealthCheck setIpv6Command(String ipv6Command) {
      this.ipv6Command = ipv6Command;
      return this;
   }

   private void checkStart() {
      if (!isStarted() && (!addresses.isEmpty() || !urls.isEmpty()) && !componentList.isEmpty()) {
         start();
      }
   }

   @Override
   public void run() {
      boolean healthy = check();

      if (healthy) {
         for (ActiveMQComponent component : componentList) {
            if (!component.isStarted()) {
               try {
                  ActiveMQUtilLogger.LOGGER.startingService(component.toString());
                  component.start();
               } catch (Exception e) {
                  ActiveMQUtilLogger.LOGGER.errorStartingComponent(e, component.toString());
               }
            }
         }
      } else {
         for (ActiveMQComponent component : componentList) {
            if (component.isStarted()) {
               try {
                  ActiveMQUtilLogger.LOGGER.stoppingService(component.toString());
                  component.stop();
               } catch (Exception e) {
                  ActiveMQUtilLogger.LOGGER.errorStoppingComponent(e, component.toString());
               }
            }
         }
      }

   }

   /**
    * @return true if no checks were done or if one address/url responds; false if all addresses/urls fail
    */
   public boolean check() {
      if (isEmpty()) {
         return true;
      }

      for (InetAddress address : addresses) {
         if (check(address)) {
            return true;
         }
      }

      for (URL url : urls) {
         if (check(url)) {
            return true;
         }
      }

      return false;
   }

   public boolean check(InetAddress address) {
      try {
         if (address.isReachable(networkInterface, 0, networkTimeout)) {
            if (logger.isTraceEnabled()) {
               logger.tracef(address + " OK");
            }
            return true;
         } else {
            return purePing(address);
         }
      } catch (Exception e) {
         ActiveMQUtilLogger.LOGGER.failedToCheckAddress(e, address == null ? " " : address.toString());
         return false;
      }
   }

   public boolean purePing(InetAddress address) throws IOException, InterruptedException {
      long timeout = Math.max(1, TimeUnit.MILLISECONDS.toSeconds(networkTimeout));
      // it did not work with a simple isReachable, it could be because there's no root access, so we will try ping executable

      if (logger.isTraceEnabled()) {
         logger.trace("purePing on canonical address " + address.getCanonicalHostName());
      }
      ProcessBuilder processBuilder;
      if (address instanceof Inet6Address) {
         processBuilder = buildProcess(ipv6Command, timeout, address.getCanonicalHostName());
      } else {
         processBuilder = buildProcess(ipv4Command, timeout, address.getCanonicalHostName());
      }

      Process pingProcess = processBuilder.start();

      readStream(pingProcess.getInputStream(), false);
      readStream(pingProcess.getErrorStream(), true);

      return pingProcess.waitFor() == 0;
   }


   private ProcessBuilder buildProcess(String expressionCommand, long timeout, String host) {
      String command = String.format(expressionCommand, timeout, host);

      if (logger.isDebugEnabled()) {
         logger.debug("executing ping:: " + command);
      }

      ProcessBuilder builder = new ProcessBuilder(command.split(" "));

      return builder;
   }

   private void readStream(InputStream stream, boolean error) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

      String inputLine;
      while ((inputLine = reader.readLine()) != null) {
         if (error) {
            ActiveMQUtilLogger.LOGGER.failedToReadFromStream(inputLine);
         } else {
            logger.debug(inputLine);
         }
      }

      reader.close();
   }

   public boolean check(URL url) {
      try {
         URLConnection connection = url.openConnection();
         connection.setReadTimeout(networkTimeout);
         InputStream is = connection.getInputStream();
         is.close();
         return true;
      } catch (Exception e) {
         ActiveMQUtilLogger.LOGGER.failedToCheckURL(e, url == null ? " " : url.toString());
         return false;
      }
   }

   public boolean isEmpty() {
      return addresses.isEmpty() && urls.isEmpty();
   }
}
