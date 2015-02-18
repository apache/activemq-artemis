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
import org.apache.activemq.utils.uri.SchemaConstants;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class InVMTransportConfigurationSchema extends AbstractTransportConfigurationSchema
{
   @Override
   public String getSchemaName()
   {
      return SchemaConstants.VM;
   }

   @Override
   protected List<TransportConfiguration> internalNewObject(URI uri, Map<String, String> query, String name) throws Exception
   {
      List<TransportConfiguration> configurations = new ArrayList<>();
      configurations.add(createTransportConfiguration(uri, name, getFactoryName()));
      return configurations;
   }

   @Override
   protected URI internalNewURI(List<TransportConfiguration> bean) throws Exception
   {
      return null;
   }

   protected String getFactoryName()
   {
      return "org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory";
   }

   public static TransportConfiguration createTransportConfiguration(URI uri, String name, String factoryName)
   {
      Map<String, Object> inVmTransportConfig = new HashMap<>();
      inVmTransportConfig.put("serverId", uri.getHost());
      return new TransportConfiguration(factoryName, inVmTransportConfig, name);
   }
}
