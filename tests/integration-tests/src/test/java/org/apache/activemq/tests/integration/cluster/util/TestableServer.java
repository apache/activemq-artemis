/**
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
package org.apache.activemq.tests.integration.cluster.util;

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.ActiveMQServer;

/**
 * A TestServer
 * @author jmesnil
 */
public interface TestableServer extends ActiveMQComponent
{
   ActiveMQServer getServer();

   void stop() throws Exception;

   void setIdentity(String identity);

   CountDownLatch crash(ClientSession... sessions) throws Exception;

   CountDownLatch crash(boolean waitFailure, ClientSession... sessions) throws Exception;

   boolean isActive();

   void addInterceptor(Interceptor interceptor);

   void removeInterceptor(Interceptor interceptor);
}
