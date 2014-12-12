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

package org.apache.activemq.uri;

import java.io.PrintStream;
import java.net.URI;
import java.util.Map;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.utils.uri.URISchema;

/**
 * @author clebertsuconic
 */

public class UDPSchema extends AbstractCFSchema
{
   @Override
   public String getSchemaName()
   {
      return "udp";
   }


   @Override
   public ActiveMQConnectionFactory internalNewObject(URI uri, Map<String, String> query) throws Exception
   {
      ConnectionOptions options = newConectionOptions(uri, query);

      DiscoveryGroupConfiguration dgc = URISchema.setData(uri, new DiscoveryGroupConfiguration(), query)
         .setBroadcastEndpointFactoryConfiguration(new UDPBroadcastGroupConfiguration()
                                                      .setGroupAddress(getHost(uri))
                                                      .setGroupPort(getPort(uri))
                                                      .setLocalBindAddress(query.containsKey(TransportConstants.LOCAL_ADDRESS_PROP_NAME) ? (String) query.get(TransportConstants.LOCAL_ADDRESS_PROP_NAME) : null)
                                                      .setLocalBindPort(query.containsKey(TransportConstants.LOCAL_PORT_PROP_NAME) ? Integer.parseInt((String) query.get(TransportConstants.LOCAL_PORT_PROP_NAME)) : -1));

      if (options.isHa())
      {
         return ActiveMQJMSClient.createConnectionFactoryWithHA(dgc, options.getFactoryTypeEnum());
      }
      else
      {
         return ActiveMQJMSClient.createConnectionFactoryWithoutHA(dgc, options.getFactoryTypeEnum());
      }
   }
}
