/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.artemis.client.cdi.configuration;

import javax.enterprise.inject.Vetoed;
import java.util.Map;

@Vetoed
public class DefaultArtemisClientConfigurationImpl implements ArtemisClientConfiguration {

   private String host;
   private Integer port;
   private String url;
   private String username;
   private String password;
   private boolean ha;

   public DefaultArtemisClientConfigurationImpl() {
   }

   public DefaultArtemisClientConfigurationImpl(Map<String, Object> params) {
      host = (String) params.get("host");
      port = (Integer) params.get("port");
      url = (String) params.get("url");
      username = (String) params.get("username");
      password = (String) params.get("password");
      Boolean isHa = (Boolean) params.get("ha");
      if (isHa == null) {
         isHa = false;
      }
      ha = isHa;
   }

   @Override
   public String getHost() {
      return host;
   }

   @Override
   public Integer getPort() {
      return port;
   }

   @Override
   public String getUsername() {
      return username;
   }

   @Override
   public String getPassword() {
      return password;
   }

   @Override
   public String getUrl() {
      return url;
   }

   @Override
   public String getConnectorFactory() {
      return startEmbeddedBroker() ? IN_VM_CONNECTOR : REMOTE_CONNECTOR;
   }

   @Override
   public boolean startEmbeddedBroker() {
      return host == null && url == null;
   }

   @Override
   public boolean isHa() {
      return ha;
   }

   @Override
   public boolean hasAuthentication() {
      return getUsername() != null && getUsername().length() > 0;
   }
}
