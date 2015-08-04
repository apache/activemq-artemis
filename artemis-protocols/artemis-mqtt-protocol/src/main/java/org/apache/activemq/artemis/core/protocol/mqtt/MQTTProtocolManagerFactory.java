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

package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.List;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;

public class MQTTProtocolManagerFactory implements ProtocolManagerFactory {

   public static final String MQTT_PROTOCOL_NAME = "MQTT";

   private static final String MODULE_NAME = "artemis-mqtt-protocol";

   private static final String[] SUPPORTED_PROTOCOLS = {MQTT_PROTOCOL_NAME};

   @Override
   public ProtocolManager createProtocolManager(ActiveMQServer server,
                                                List incomingInterceptors,
                                                List outgoingInterceptors) {
      return new MQTTProtocolManager(server);
   }

   @Override
   public List filterInterceptors(List list) {
      // TODO Add support for interceptors.
      return null;
   }

   @Override
   public String[] getProtocols() {
      return SUPPORTED_PROTOCOLS;
   }

   @Override
   public String getModuleName() {
      return MODULE_NAME;
   }
}
