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

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.utils.uri.SchemaConstants;
import org.apache.activemq.utils.uri.URISchema;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class TCPServerLocatorSchema extends AbstractServerLocatorSchema
{
   @Override
   public String getSchemaName()
   {
      return SchemaConstants.TCP;
   }

   @Override
   protected ServerLocator internalNewObject(URI uri, Map<String, String> query) throws Exception
   {
      ConnectionOptions options = newConnectionOptions(uri, query);

      TransportConfiguration[] configurations = getTransportConfigurations(uri, query);

      if (options.isHa())
      {
         return ActiveMQClient.createServerLocatorWithHA(configurations);
      }
      else
      {
         return ActiveMQClient.createServerLocatorWithoutHA(configurations);
      }
   }

   public static TransportConfiguration[] getTransportConfigurations(URI uri, Map<String, String> query) throws URISyntaxException
   {
      HashMap<String, Object> props = new HashMap<>();

      URISchema.setData(uri, props, TransportConstants.ALLOWABLE_CONNECTOR_KEYS, query);
      List<TransportConfiguration> transportConfigurations = new ArrayList<>();

      transportConfigurations.add(new TransportConfiguration(NettyConnectorFactory.class.getName(),
                                                                    props,
                                                                    uri.toString()));
      String connectors = uri.getFragment();

      if (connectors != null)
      {
         String[] split = connectors.split(",");
         for (String s : split)
         {
            URI extraUri = new URI(s);
            HashMap<String, Object> newProps = new HashMap<>();
            URISchema.setData(extraUri, newProps, TransportConstants.ALLOWABLE_CONNECTOR_KEYS, query);
            URISchema.setData(extraUri, newProps, TransportConstants.ALLOWABLE_CONNECTOR_KEYS, URISchema.parseQuery(extraUri.getQuery(), null));
            transportConfigurations.add(new TransportConfiguration(NettyConnectorFactory.class.getName(),
                                                                   newProps,
                                                                   extraUri.toString()));
         }
      }
      TransportConfiguration[] configurations = new TransportConfiguration[transportConfigurations.size()];
      transportConfigurations.toArray(configurations);
      return configurations;
   }

   @Override
   protected URI internalNewURI(ServerLocator bean) throws Exception
   {
      String query = URISchema.getData(null, bean);
      TransportConfiguration[] staticConnectors = bean.getStaticTransportConfigurations();
      return getURI(query, staticConnectors);
   }

   public static URI getURI(String query, TransportConfiguration[] staticConnectors) throws Exception
   {
      if (staticConnectors == null || staticConnectors.length < 1)
      {
         throw new Exception();
      }
      StringBuilder fragment = new StringBuilder();
      for (int i = 1; i < staticConnectors.length; i++)
      {
         TransportConfiguration connector = staticConnectors[i];
         Map<String, Object> params = connector.getParams();
         URI extraUri = new URI(SchemaConstants.TCP, null, getHost(params), getPort(params), null, createQuery(params, null), null);
         if (i > 1)
         {
            fragment.append(",");
         }
         fragment.append(extraUri.toASCIIString());

      }
      Map<String, Object> params = staticConnectors[0].getParams();
      return new URI(SchemaConstants.TCP, null,  getHost(params), getPort(params), null, createQuery(params, query), fragment.toString());
   }

   private static int getPort(Map<String, Object> params)
   {
      Object port = params.get("port");
      if (port instanceof String)
      {
         return Integer.valueOf((String) port);
      }
      return port != null ? (int) port : 5445;
   }

   private static String getHost(Map<String, Object> params)
   {
      return params.get("host") != null ? (String) params.get("host") : "localhost";
   }

   private static String createQuery(Map<String, Object> params, String query)
   {
      StringBuilder cb;
      if (query == null)
      {
         cb = new StringBuilder();
      }
      else
      {
         cb = new StringBuilder(query);
      }
      for (String param : params.keySet())
      {
         if (cb.length() > 0)
         {
            cb.append("&");
         }
         cb.append(param).append("=").append(params.get(param));
      }
      return cb.toString();
   }
}
