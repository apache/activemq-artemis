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
package org.apache.activemq.core.protocol.proton.sasl;

import org.apache.activemq.core.security.SecurityStore;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.proton.plug.sasl.ServerSASLPlain;

/**
 * @author Clebert Suconic
 */

public class ActiveMQPlainSASL extends ServerSASLPlain
{

   private final ActiveMQSecurityManager securityManager;

   private final SecurityStore securityStore;


   public ActiveMQPlainSASL(SecurityStore securityStore, ActiveMQSecurityManager securityManager)
   {
      this.securityManager = securityManager;
      this.securityStore = securityStore;
   }

   @Override
   protected boolean authenticate(String user, String password)
   {
      if (securityStore.isSecurityEnabled())
      {
         return securityManager.validateUser(user, password);
      }
      else
      {
         return true;
      }
   }
}
