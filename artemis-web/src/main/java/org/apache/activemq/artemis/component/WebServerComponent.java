/*
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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.activemq.artemis.ActiveMQWebLogger;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

public class WebServerComponent implements ExternalComponent {

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;
   private URI uri;
   private String jolokiaUrl;

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      webServerConfig = (WebServerDTO) config;
      uri = new URI(webServerConfig.bind);
      server = new Server();
      String scheme = uri.getScheme();
      ServerConnector connector = null;

      if ("https".equals(scheme)) {
         SslContextFactory sslFactory = new SslContextFactory();
         sslFactory.setKeyStorePath(webServerConfig.keyStorePath == null ? artemisInstance + "/etc/keystore.jks" : webServerConfig.keyStorePath);
         sslFactory.setKeyStorePassword(webServerConfig.keyStorePassword == null ? "password" : webServerConfig.keyStorePassword);
         if (webServerConfig.clientAuth != null) {
            sslFactory.setNeedClientAuth(webServerConfig.clientAuth);
            if (webServerConfig.clientAuth) {
               sslFactory.setTrustStorePath(webServerConfig.trustStorePath);
               sslFactory.setTrustStorePassword(webServerConfig.trustStorePassword);
            }
         }

         SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslFactory, "HTTP/1.1");

         HttpConfiguration https = new HttpConfiguration();
         https.addCustomizer(new SecureRequestCustomizer());
         HttpConnectionFactory httpFactory = new HttpConnectionFactory(https);

         connector = new ServerConnector(server, sslConnectionFactory, httpFactory);

      } else {
         connector = new ServerConnector(server);
      }
      connector.setPort(uri.getPort());
      connector.setHost(uri.getHost());

      server.setConnectors(new Connector[]{connector});

      handlers = new HandlerList();

      Path warDir = Paths.get(artemisHome != null ? artemisHome : ".").resolve(webServerConfig.path).toAbsolutePath();

      if (webServerConfig.apps != null) {
         for (AppDTO app : webServerConfig.apps) {
            deployWar(app.url, app.war, warDir);
            if (app.war.startsWith("jolokia")) {
               jolokiaUrl = webServerConfig.bind + "/" + app.url;
            }
         }
      }

      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setResourceBase(warDir.toString());
      resourceHandler.setDirectoriesListed(true);
      resourceHandler.setWelcomeFiles(new String[]{"index.html"});

      DefaultHandler defaultHandler = new DefaultHandler();
      defaultHandler.setServeIcon(false);

      handlers.addHandler(resourceHandler);
      handlers.addHandler(defaultHandler);
      server.setHandler(handlers);
   }

   @Override
   public void start() throws Exception {
      server.start();
      ActiveMQWebLogger.LOGGER.webserverStarted(webServerConfig.bind);
      if (jolokiaUrl != null) {
         ActiveMQWebLogger.LOGGER.jolokiaAvailable(jolokiaUrl);
      }
   }

   @Override
   public void stop() throws Exception {
      server.stop();
   }

   @Override
   public boolean isStarted() {
      return server != null && server.isStarted();
   }

   private void deployWar(String url, String warFile, Path warDirectory) throws IOException {
      WebAppContext webapp = new WebAppContext();
      if (url.startsWith("/")) {
         webapp.setContextPath(url);
      } else {
         webapp.setContextPath("/" + url);
      }

      webapp.setWar(warDirectory.resolve(warFile).toString());
      handlers.addHandler(webapp);
   }
}
