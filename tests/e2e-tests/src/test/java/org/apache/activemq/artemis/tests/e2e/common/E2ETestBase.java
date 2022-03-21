/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.e2e.common;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;

public class E2ETestBase extends ActiveMQTestBase {

   public static final String basedir = System.getProperty("basedir");

   protected static final void recreateBrokerDirectory(final String homeInstance) {
      recreateDirectory(homeInstance + "/data");
      recreateDirectory(homeInstance + "/log");
   }

   public boolean waitForServerToStart(String uri, String username, String password, long timeout) throws InterruptedException {
      long realTimeout = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < realTimeout) {
         try (ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactory(uri, null)) {
            cf.createConnection(username, password).close();
            System.out.println("server " + uri + " started");
         } catch (Exception e) {
            System.out.println("awaiting server " + uri + " start at ");
            Thread.sleep(500);
            continue;
         }
         return true;
      }

      return false;
   }
}
