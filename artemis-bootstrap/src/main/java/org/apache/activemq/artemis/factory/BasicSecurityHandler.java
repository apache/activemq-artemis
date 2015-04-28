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
package org.apache.activemq.artemis.factory;

import java.io.File;

import org.apache.activemq.artemis.core.config.impl.FileSecurityConfiguration;
import org.apache.activemq.artemis.dto.BasicSecurityDTO;
import org.apache.activemq.artemis.dto.SecurityDTO;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;

public class BasicSecurityHandler implements SecurityHandler
{


   static String fixupFileURI(String value)
   {
      if (value != null && value.startsWith("file:"))
      {
         value = value.substring("file:".length());
         value = new File(value).toURI().toString();
      }
      return value;
   }

   @Override
   public ActiveMQSecurityManager createSecurityManager(SecurityDTO security) throws Exception
   {
      BasicSecurityDTO fileSecurity = (BasicSecurityDTO) security;
      String home = System.getProperty("activemq.home");
      FileSecurityConfiguration securityConfiguration = new FileSecurityConfiguration(fixupFileURI(fileSecurity.users),
                                                                                      fixupFileURI(fileSecurity.roles),
                                                                                      fileSecurity.defaultUser,
                                                                                      fileSecurity.maskPassword,
                                                                                      fileSecurity.passwordCodec);
      securityConfiguration.start();
      return new ActiveMQSecurityManagerImpl(securityConfiguration);
   }
}
