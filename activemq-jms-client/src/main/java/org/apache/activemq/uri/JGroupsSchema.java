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

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.JGroupsBroadcastGroupConfiguration;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.utils.uri.URISchema;

/**
 * @author clebertsuconic
 */

public class JGroupsSchema extends AbstractCFSchema
{
   @Override
   public String getSchemaName()
   {
      return "jgroups";
   }


   @Override
   public ActiveMQConnectionFactory internalNewObject(URI uri, Map<String, String> query) throws Exception
   {
      ConnectionOptions options = newConectionOptions(uri, query);

      System.out.println("authority = " + uri.getAuthority() + " path = " + uri.getPath());

      JGroupsBroadcastGroupConfiguration jgroupsConfig = new JGroupsBroadcastGroupConfiguration(uri.getAuthority(), uri.getPath() != null ? uri.getPath() : UUID.randomUUID().toString());

      URISchema.setData(uri, jgroupsConfig, query);


      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration().setBroadcastEndpointFactoryConfiguration(jgroupsConfig);

      URISchema.setData(uri, dcConfig, query);


      if (options.isHa())
      {
         return ActiveMQJMSClient.createConnectionFactoryWithHA(dcConfig, options.getFactoryTypeEnum());
      }
      else
      {
         return ActiveMQJMSClient.createConnectionFactoryWithoutHA(dcConfig, options.getFactoryTypeEnum());
      }
   }
}
