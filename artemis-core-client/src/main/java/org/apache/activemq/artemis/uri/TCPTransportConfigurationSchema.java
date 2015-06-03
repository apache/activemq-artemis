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
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.utils.uri.SchemaConstants;
import org.apache.activemq.artemis.utils.uri.URISchema;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TCPTransportConfigurationSchema extends AbstractTransportConfigurationSchema
{
   private final Set<String> allowableProperties;

   public TCPTransportConfigurationSchema(Set<String> allowableProperties)
   {
      this.allowableProperties = allowableProperties;
   }

   @Override
   public String getSchemaName()
   {
      return SchemaConstants.TCP;
   }

   @Override
   protected List<TransportConfiguration> internalNewObject(URI uri, Map<String, String> query, String name) throws Exception
   {
      return getTransportConfigurations(uri, query, allowableProperties, name, getFactoryName(uri));
   }

   @Override
   protected URI internalNewURI(List<TransportConfiguration> bean) throws Exception
   {
      return null;
   }

   public static List<TransportConfiguration> getTransportConfigurations(URI uri, Map<String, String> query, Set<String> allowableProperties, String name, String factoryName) throws URISyntaxException
   {
      HashMap<String, Object> props = new HashMap<>();

      URISchema.setData(uri, props, allowableProperties, query);
      List<TransportConfiguration> transportConfigurations = new ArrayList<>();

      transportConfigurations.add(new TransportConfiguration(factoryName,
                                                             props,
                                                             name));
      String connectors = uri.getFragment();

      if (connectors != null)
      {
         String[] split = connectors.split(",");
         for (String s : split)
         {
            URI extraUri = new URI(s);
            HashMap<String, Object> newProps = new HashMap<>();
            URISchema.setData(extraUri, newProps, allowableProperties, query);
            URISchema.setData(extraUri, newProps, allowableProperties, URISchema.parseQuery(extraUri.getQuery(), null));
            transportConfigurations.add(new TransportConfiguration(factoryName,
                                                                   newProps,
                                                                   name + ":" + extraUri.toString()));
         }
      }
      return transportConfigurations;
   }

   protected String getFactoryName(URI uri)
   {
      //here for backwards compatibility
      if (uri.getPath() != null && uri.getPath().contains("hornetq"))
      {
         return "org.hornetq.core.remoting.impl.netty.NettyConnectorFactory";
      }
      return NettyConnectorFactory.class.getName();
   }
}
