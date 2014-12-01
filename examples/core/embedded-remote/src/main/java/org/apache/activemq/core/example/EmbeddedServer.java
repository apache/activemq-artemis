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
package org.apache.activemq.core.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;

/**
 * An EmbeddedServer
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class EmbeddedServer
{

	public static void main(final String arg[]) throws Exception
   {
      try
      {
         // Step 1. Create the Configuration, and set the properties accordingly
         Configuration configuration = new ConfigurationImpl();
         //we only need this for the server lock file
         configuration.setJournalDirectory("target/data/journal");
         configuration.setPersistenceEnabled(false);
         configuration.setSecurityEnabled(false);
         /**
          * this map with configuration values is not necessary (it configures the default values).
          * If you want to modify it to run the example in two different hosts, remember to also
          * modify the client's Connector at {@link EmbeddedRemoteExample}.
          */
         Map<String, Object> map = new HashMap<String, Object>();
         map.put("host", "localhost");
         map.put("port", 5445);

         TransportConfiguration transpConf = new TransportConfiguration(NettyAcceptorFactory.class.getName(),map);

         HashSet<TransportConfiguration> setTransp = new HashSet<TransportConfiguration>();
         setTransp.add(transpConf);

         configuration.setAcceptorConfigurations(setTransp);

         // Step 2. Create and start the server
         ActiveMQServer server = ActiveMQServers.newActiveMQServer(configuration);
         server.start();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw e;
      }
   }
}
