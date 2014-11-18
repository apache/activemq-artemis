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
package org.apache.activemq.rest.integration;

import org.apache.activemq.core.server.embedded.EmbeddedActiveMQ;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.apache.activemq.rest.MessageServiceManager;
import org.jboss.resteasy.test.TestPortProvider;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedRestActiveMQ
{
   protected TJWSEmbeddedJaxrsServer tjws = new TJWSEmbeddedJaxrsServer();
   protected EmbeddedActiveMQ embeddedActiveMQ;
   protected MessageServiceManager manager = new MessageServiceManager();

   public EmbeddedRestActiveMQ()
   {
      int port = TestPortProvider.getPort();
      tjws.setPort(port);
      tjws.setRootResourcePath("");
      tjws.setSecurityDomain(null);
      initEmbeddedActiveMQ();
   }

   protected void initEmbeddedActiveMQ()
   {
      embeddedActiveMQ = new EmbeddedActiveMQ();
   }

   public TJWSEmbeddedJaxrsServer getTjws()
   {
      return tjws;
   }

   public void setTjws(TJWSEmbeddedJaxrsServer tjws)
   {
      this.tjws = tjws;
   }

   public EmbeddedActiveMQ getEmbeddedActiveMQ()
   {
      return embeddedActiveMQ;
   }

   public MessageServiceManager getManager()
   {
      return manager;
   }

   public void start() throws Exception
   {
      embeddedActiveMQ.start();
      tjws.start();
      manager.start();
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getTopicManager().getDestination());
   }

   public void stop() throws Exception
   {
      try
      {
         tjws.stop();
      }
      catch (Exception e)
      {
      }
      try
      {
         manager.stop();
      }
      catch (Exception e)
      {
      }
      embeddedActiveMQ.stop();
   }

}
