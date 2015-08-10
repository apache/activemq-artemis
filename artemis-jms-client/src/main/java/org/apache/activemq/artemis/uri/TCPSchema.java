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
package org.apache.activemq.artemis.uri;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.uri.SchemaConstants;
import org.apache.activemq.artemis.utils.uri.URISchema;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class TCPSchema extends AbstractCFSchema {

   @Override
   public String getSchemaName() {
      return SchemaConstants.TCP;
   }

   @Override
   protected ActiveMQConnectionFactory internalNewObject(URI uri,
                                                         Map<String, String> query,
                                                         String name) throws Exception {
      JMSConnectionOptions options = newConectionOptions(uri, query);

      List<TransportConfiguration> configurations = TCPTransportConfigurationSchema.getTransportConfigurations(uri, query, TransportConstants.ALLOWABLE_CONNECTOR_KEYS, name, NettyConnectorFactory.class.getName());

      TransportConfiguration[] tcs = new TransportConfiguration[configurations.size()];

      configurations.toArray(tcs);

      ActiveMQConnectionFactory factory;

      if (options.isHa()) {
         factory = ActiveMQJMSClient.createConnectionFactoryWithHA(options.getFactoryTypeEnum(), tcs);
      }
      else {
         factory = ActiveMQJMSClient.createConnectionFactoryWithoutHA(options.getFactoryTypeEnum(), tcs);
      }

      return URISchema.setData(uri, factory, query);
   }

   @Override
   protected URI internalNewURI(ActiveMQConnectionFactory bean) throws Exception {
      String query = URISchema.getData(null, bean);
      TransportConfiguration[] staticConnectors = bean.getStaticConnectors();
      return TCPServerLocatorSchema.getURI(query, staticConnectors);
   }
}
