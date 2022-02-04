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

package org.apache.activemq.artemis.tests.integration.mqtt5.spec;

import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.jboss.logging.Logger;
import org.junit.Ignore;

/**
 * Fulfilled by client or Netty codec (i.e. not tested explicitly here, but tested implicitly through all the tests using TCP):
 *
 * [MQTT-4.2.0-1] A Client or Server MUST support the use of one or more underlying transport protocols that provide an ordered, lossless, stream of bytes from the Client to Server and Server to Client.
 */

@Ignore
public class NetworkConnectionTests extends MQTT5TestSupport {

   private static final Logger log = Logger.getLogger(NetworkConnectionTests.class);

   public NetworkConnectionTests(String protocol) {
      super(protocol);
   }
}