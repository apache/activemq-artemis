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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnectionManager;
import org.apache.activemq.artemis.spi.core.protocol.AbstractProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.utils.uri.BeanSupport;
import org.osgi.service.component.annotations.Component;

@Component(service = ProtocolManagerFactory.class)
public class ProtonProtocolManagerFactory extends AbstractProtocolManagerFactory<AmqpInterceptor> {

   public static final String AMQP_PROTOCOL_NAME = "AMQP";

   private static final String MODULE_NAME = "artemis-amqp-protocol";

   private static String[] SUPPORTED_PROTOCOLS = {AMQP_PROTOCOL_NAME};

   @Override
   public Persister<Message>[] getPersister() {

      Persister[] persisters = new Persister[]{AMQPMessagePersister.getInstance(), AMQPMessagePersisterV2.getInstance(), AMQPLargeMessagePersister.getInstance(), AMQPMessagePersisterV3.getInstance()};
      return persisters;
   }

   @Override
   public ProtocolManager createProtocolManager(ActiveMQServer server,
                                                final Map<String, Object> parameters,
                                                List<BaseInterceptor> incomingInterceptors,
                                                List<BaseInterceptor> outgoingInterceptors) throws Exception {
      BeanSupport.stripPasswords(parameters);
      return BeanSupport.setData(new ProtonProtocolManager(this, server, incomingInterceptors, outgoingInterceptors), parameters);
   }

   @Override
   public List<AmqpInterceptor> filterInterceptors(List<BaseInterceptor> interceptors) {
      return internalFilterInterceptors(AmqpInterceptor.class, interceptors);
   }

   @Override
   public String[] getProtocols() {
      return SUPPORTED_PROTOCOLS;
   }

   @Override
   public String getModuleName() {
      return MODULE_NAME;
   }

   /** AMQP integration with the broker on this case needs to be soft.
    *  As the broker may choose to not load the AMQP Protocol */
   @Override
   public void loadProtocolServices(ActiveMQServer server, List<ActiveMQComponent> services) {
      List<AMQPBrokerConnectConfiguration> amqpServicesConfiguration = server.getConfiguration().getAMQPConnection();
      if (amqpServicesConfiguration != null && amqpServicesConfiguration.size() > 0) {
         AMQPBrokerConnectionManager bridgeService = new AMQPBrokerConnectionManager(this, amqpServicesConfiguration, server);
         services.add(bridgeService);
      }
   }
}
