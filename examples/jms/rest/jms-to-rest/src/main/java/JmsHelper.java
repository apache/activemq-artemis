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
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.core.config.FileDeploymentManager;
import org.apache.activemq.core.config.impl.FileConfiguration;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.jms.client.ActiveMQJMSConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

public class JmsHelper
{
   public static ConnectionFactory createConnectionFactory(String configFile) throws Exception
   {
      FileConfiguration config = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(configFile);
      deploymentManager.addDeployable(config);
      deploymentManager.readConfiguration();
      TransportConfiguration transport = config.getConnectorConfigurations().get("netty-connector");
      return new ActiveMQJMSConnectionFactory(ActiveMQClient.createServerLocatorWithoutHA(transport));

   }

}
