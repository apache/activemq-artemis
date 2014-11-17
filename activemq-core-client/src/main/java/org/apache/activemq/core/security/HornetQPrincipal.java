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

package org.apache.activemq6.core.security;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         1/30/12
 */
public class HornetQPrincipal
{
   private final String userName;

   private final String password;

   public HornetQPrincipal(String userName, String password)
   {
      this.userName = userName;
      this.password = password;
   }

   public String getUserName()
   {
      return userName;
   }

   public String getPassword()
   {
      return password;
   }
}
