/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.factory;

import org.hornetq.dto.SecurityDTO;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.FactoryFinder;

import javax.xml.bind.annotation.XmlRootElement;

public class SecurityManagerFactory
{

   public static HornetQSecurityManager create(SecurityDTO config) throws Exception
   {
      if (config != null)
      {
         FactoryFinder finder = new FactoryFinder("META-INF/services/org/hornetq/security/");
         HornetQSecurityManager manager = (HornetQSecurityManager)finder.newInstance(config.getClass().getAnnotation(XmlRootElement.class).name());
         return manager;
      }
      else
      {
         throw new Exception("No security manager configured!");
      }
   }

}
