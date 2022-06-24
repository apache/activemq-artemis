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
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * This will make sure any properties changed through tests are cleaned up between tests.
 */
public class PortCheckRule extends TestWatcher {

   final int[] port;

   public PortCheckRule(int... port) {
      this.port = port;
   }

   @Override
   protected void starting(Description description) {
      for (int p : port) {
         if (!checkAvailable(p)) {
            Assert.fail("a previous test is using port " + p + " on " + description);
         }
      }
   }

   @Override
   protected void finished(Description description) {
      for (int p : port) {
         if (!checkAvailable(p)) {
            Assert.fail(description + " has left a server socket open on port " + p);
         }
      }
   }

   public static boolean checkAvailable(int port) {
      ServerSocket s = null;
      try {
         s = new ServerSocket();
         s.bind(new InetSocketAddress("localhost", 61616));
         return true;
      } catch (IOException e) {
         e.printStackTrace();
         return false;
      } finally {
         try {
            s.close();
         } catch (Throwable ignored) {
         }
      }
   }

}
