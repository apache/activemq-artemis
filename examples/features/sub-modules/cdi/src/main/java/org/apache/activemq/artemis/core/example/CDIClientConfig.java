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

package org.apache.activemq.artemis.core.example;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.artemis.client.cdi.configuration.ArtemisClientConfiguration;
import org.apache.deltaspike.core.api.config.ConfigProperty;

@ApplicationScoped
public class CDIClientConfig implements ArtemisClientConfiguration {
   @Inject
   @ConfigProperty(name = "username")
   private String username;

   @Inject
   @ConfigProperty(name = "password")
   private String password;

   @Inject
   @ConfigProperty(name = "url")
   private String url;


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
   public String getHost() {
      return null;
   }

   @Override
   public Integer getPort() {
      return null;
   }

   @Override
   public String getConnectorFactory() {
      return "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";
   }

   @Override
   public boolean startEmbeddedBroker() {
      return false;
   }

   @Override
   public boolean isHa() {
      return false;
   }

   @Override
   public boolean hasAuthentication() {
      return false;
   }
}
