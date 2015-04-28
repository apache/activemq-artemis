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
package org.apache.activemq.artemis.component;

import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

import java.net.URI;

public class WebServerComponent implements ExternalComponent
{

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;

   @Override
   public void configure(ComponentDTO config, String activemqHome) throws Exception
   {
      webServerConfig = (WebServerDTO)config;
      String path = webServerConfig.path.startsWith("/") ? webServerConfig.path : "/" + webServerConfig.path;
      URI uri = new URI(webServerConfig.bind);
      server = new Server();
      org.eclipse.jetty.server.nio.SelectChannelConnector connector = new SelectChannelConnector();
      connector.setPort(uri.getPort());
      connector.setHost(uri.getHost());

      server.setConnectors(new Connector[]{connector});

      handlers = new HandlerList();

      if (webServerConfig.apps != null)
      {
         for (AppDTO app : webServerConfig.apps)
         {
            deployWar(app.url, app.war, activemqHome, path);
         }
      }

      WebAppContext handler = new WebAppContext();
      handler.setContextPath("/");
      handler.setResourceBase(activemqHome + path);
      handler.setLogUrlOnStart(true);

      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setResourceBase(activemqHome + path);
      resourceHandler.setDirectoriesListed(true);
      resourceHandler.setWelcomeFiles(new String[]{"index.html"});

      DefaultHandler defaultHandler = new DefaultHandler();
      defaultHandler.setServeIcon(false);

      handlers.addHandler(resourceHandler);
      handlers.addHandler(defaultHandler);
      server.setHandler(handlers);
   }

   public void start() throws Exception
   {
      server.start();

      System.out.println("HTTP Server started at " + webServerConfig.bind);
   }

   public void stop() throws Exception
   {
      server.stop();
   }

   public boolean isStarted()
   {
      return false;
   }

   private void deployWar(String url, String warURL, String activeMQHome, String path)
   {
      WebAppContext webapp = new WebAppContext();
      if (url.startsWith("/"))
      {
         webapp.setContextPath(url);
      }
      else
      {
         webapp.setContextPath("/" + url);
      }
      webapp.setWar(activeMQHome + path + "/" + warURL);
      handlers.addHandler(webapp);
   }
}
