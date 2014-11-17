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
package org.apache.activemq.service;

import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 19, 2009
 */
public class HornetQJMSStarterService implements HornetQJMSStarterServiceMBean
{
   HornetQStarterServiceMBean service;

   private JMSServerManagerImpl jmsServerManager;

   public void create() throws Exception
   {
      jmsServerManager = new JMSServerManagerImpl(service.getServer());
   }

   public void start() throws Exception
   {
      jmsServerManager.start();
   }

   public void stop() throws Exception
   {
      jmsServerManager.stop();
   }

   public void setHornetQServer(final HornetQStarterServiceMBean service)
   {
      this.service = service;
   }
}
