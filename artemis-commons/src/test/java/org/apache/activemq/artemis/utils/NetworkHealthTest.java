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

package org.apache.activemq.artemis.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

public class NetworkHealthTest {

   private static final InetAddress INVALID_ADDRESS;

   private static String IPV6_LOCAL = "::1";

   static {
      InetAddress address = null;
      try {
         address = InetAddress.getByName("203.0.113.1");
      } catch (Exception e) {
         e.printStackTrace();
      }
      INVALID_ADDRESS = address;
   }

   Set<NetworkHealthCheck> list = new HashSet<>();

   NetworkHealthCheck addCheck(NetworkHealthCheck check) {
      list.add(check);
      return check.setIgnoreLoopback(true);
   }

   HttpServer httpServer;

   final ReusableLatch latch = new ReusableLatch(1);

   ActiveMQComponent component = new ActiveMQComponent() {
      boolean started = true;

      @Override
      public void start() throws Exception {
         started = true;
         latch.countDown();
      }

      @Override
      public void stop() throws Exception {
         started = false;
         latch.countDown();
      }

      @Override
      public boolean isStarted() {
         return started;
      }
   };

   @Before
   public void before() throws Exception {
      latch.setCount(1);
   }

   private void startHTTPServer() throws IOException {
      Assert.assertNull(httpServer);
      InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8787);
      httpServer = HttpServer.create(address, 100);
      httpServer.start();
      httpServer.createContext("/", new HttpHandler() {
         @Override
         public void handle(HttpExchange t) throws IOException {
            String response = "<html><body><b>This is a unit test</b></body></html>";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
         }
      });
   }

   private void stopHTTPServer() {
      if (httpServer != null) {
         try {
            httpServer.stop(0);
         } catch (Throwable ignored) {
         }
         httpServer = null;
      }
   }

   @After
   public void after() {
      stopHTTPServer();
      for (NetworkHealthCheck check : this.list) {
         check.stop();
      }
   }

   @Test
   public void testCheck6() throws Exception {
      assumeTrue(purePingWorks(IPV6_LOCAL));
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));
      check.addComponent(component);

      InetAddress address = InetAddress.getByName(IPV6_LOCAL);

      Assert.assertTrue(address instanceof Inet6Address);

      Assert.assertTrue(check.purePing(address));

      Assert.assertTrue(check.check(address));
   }

   @Test
   public void testParseSpaces() throws Exception {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));

      // using two addresses for URI and localhost
      check.parseAddressList("localhost, , 127.0.0.2").parseURIList("http://www.redhat.com, , http://www.apache.org");
      Assert.assertEquals(2, check.getAddresses().size());
      Assert.assertEquals(2, check.getUrls().size());
   }

   @Test
   public void testParseLogger() throws Exception {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));

      // using two addresses for URI and localhost
      check.parseAddressList("localhost, , 127.0.0.2").parseURIList("http://www.redhat.com, , http://www.apache.org");
      Assert.assertEquals(2, check.getAddresses().size());
      Assert.assertEquals(2, check.getUrls().size());
   }

   @Test
   public void testPings() throws Exception {
      assumeTrue(purePingWorks("127.0.0.1"));
      doCheck("127.0.0.1");
   }

   private void doCheck(String localaddress) throws Exception {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));
      check.addComponent(component);

      // Any external IP, to make sure we would use a PING
      InetAddress address = InetAddress.getByName(localaddress);

      Assert.assertTrue(check.check(address));

      Assert.assertTrue(check.purePing(address));

      Assert.assertFalse(check.purePing(INVALID_ADDRESS));

   }

   @Test
   public void testPingsIPV6() throws Exception {
      assumeTrue(purePingWorks(IPV6_LOCAL));
      doCheck(IPV6_LOCAL);
   }

   private boolean purePingWorks(String localaddress) throws Exception {
      try {
         return addCheck(new NetworkHealthCheck(null, 100, 100)).purePing(InetAddress.getByName(localaddress));
      } catch (Exception e) {
         return false;
      }
   }

   @Test
   public void testCheckNoNodes() throws Exception {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck());
      Assert.assertTrue(check.check());
   }

   @Test
   public void testCheckUsingHTTP() throws Exception {

      startHTTPServer();

      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 1000));

      Assert.assertTrue(check.check(new URL("http://localhost:8787")));

      stopHTTPServer();

      Assert.assertFalse(check.check(new URL("http://localhost:8787")));

      check.addComponent(component);

      URL url = new URL("http://localhost:8787");
      Assert.assertFalse(check.check(url));

      startHTTPServer();

      Assert.assertTrue(check.check(url));

      check.addURL(url);

      Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
      Assert.assertTrue(component.isStarted());

      // stopping the web server should stop the component
      stopHTTPServer();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(component.isStarted());

      latch.setCount(1);

      startHTTPServer();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(component.isStarted());

   }

}
