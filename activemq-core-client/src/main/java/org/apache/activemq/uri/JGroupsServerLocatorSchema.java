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

import org.apache.activemq.api.core.BroadcastEndpointFactory;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.JGroupsFileBroadcastEndpointFactory;
import org.apache.activemq.api.core.JGroupsPropertiesBroadcastEndpointFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.utils.uri.SchemaConstants;
import org.apache.activemq.utils.uri.URISchema;

import java.io.NotSerializableException;
import java.net.URI;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class JGroupsServerLocatorSchema extends AbstractServerLocatorSchema
{
   @Override
   public String getSchemaName()
   {
      return SchemaConstants.JGROUPS;
   }

   @Override
   protected ServerLocator internalNewObject(URI uri, Map<String, String> query) throws Exception
   {
      ConnectionOptions options = newConnectionOptions(uri, query);

      DiscoveryGroupConfiguration dcConfig = getDiscoveryGroupConfiguration(uri, query);

      if (options.isHa())
      {
         return ActiveMQClient.createServerLocatorWithHA(dcConfig);
      }
      else
      {
         return ActiveMQClient.createServerLocatorWithoutHA(dcConfig);
      }
   }

   @Override
   protected URI internalNewURI(ServerLocator bean) throws Exception
   {
      DiscoveryGroupConfiguration dgc = bean.getDiscoveryGroupConfiguration();
      BroadcastEndpointFactory endpoint =  dgc.getBroadcastEndpointFactory();
      String auth;
      if (endpoint instanceof JGroupsFileBroadcastEndpointFactory)
      {
         auth = ((JGroupsFileBroadcastEndpointFactory) endpoint).getChannelName();
      }
      else if (endpoint instanceof JGroupsPropertiesBroadcastEndpointFactory)
      {
         auth = ((JGroupsPropertiesBroadcastEndpointFactory) endpoint).getChannelName();
      }
      else
      {
         throw new NotSerializableException(endpoint + "not serializable");
      }
      String query = URISchema.getData(null, bean, dgc, endpoint);
      dgc.setBroadcastEndpointFactory(endpoint);
      return new URI(SchemaConstants.JGROUPS, null,  auth, -1, null, query, null);
   }

   public static DiscoveryGroupConfiguration getDiscoveryGroupConfiguration(URI uri, Map<String, String> query) throws Exception
   {
      BroadcastEndpointFactory endpointFactory;
      if (query.containsKey("file"))
      {
         endpointFactory = new JGroupsFileBroadcastEndpointFactory().setChannelName(uri.getAuthority());
      }
      else
      {
         endpointFactory = new JGroupsPropertiesBroadcastEndpointFactory().setChannelName(uri.getAuthority());
      }

      URISchema.setData(uri, endpointFactory, query);

      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration().setBroadcastEndpointFactory(endpointFactory);

      URISchema.setData(uri, dcConfig, query);
      return dcConfig;
   }
}
