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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.ActiveMQWebLogger;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.TimeUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jboss.logging.Logger;

public class WebServerComponent implements ExternalComponent {

   private static final Logger logger = Logger.getLogger(WebServerComponent.class);

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;
   private URI uri;
   private String consoleUrl;
   private List<WebAppContext> webContexts;
   private ServerConnector connector;

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      webServerConfig = (WebServerDTO) config;
      uri = new URI(webServerConfig.bind);
      server = new Server();
      String scheme = uri.getScheme();

      if ("https".equals(scheme)) {
         SslContextFactory sslFactory = new SslContextFactory();
         sslFactory.setKeyStorePath(webServerConfig.keyStorePath == null ? artemisInstance + "/etc/keystore.jks" : webServerConfig.keyStorePath);
         sslFactory.setKeyStorePassword(webServerConfig.getKeyStorePassword() == null ? "password" : webServerConfig.getKeyStorePassword());
         if (webServerConfig.clientAuth != null) {
            sslFactory.setNeedClientAuth(webServerConfig.clientAuth);
            if (webServerConfig.clientAuth) {
               sslFactory.setTrustStorePath(webServerConfig.trustStorePath);
               sslFactory.setTrustStorePassword(webServerConfig.getTrustStorePassword());
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

      Path homeWarDir = Paths.get(artemisHome != null ? artemisHome : ".").resolve(webServerConfig.path).toAbsolutePath();
      Path instanceWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve(webServerConfig.path).toAbsolutePath();

      if (webServerConfig.apps != null && webServerConfig.apps.size() > 0) {
         webContexts = new ArrayList<>();
         for (AppDTO app : webServerConfig.apps) {
            Path dirToUse = homeWarDir;
            if (new File(instanceWarDir.toFile().toString() + File.separator + app.war).exists()) {
               dirToUse = instanceWarDir;
            }
            WebAppContext webContext = deployWar(app.url, app.war, dirToUse);
            webContexts.add(webContext);
            if (app.war.startsWith("console")) {
               consoleUrl = webServerConfig.bind + "/" + app.url;
            }
         }
      }

      ResourceHandler homeResourceHandler = new ResourceHandler();
      homeResourceHandler.setResourceBase(homeWarDir.toString());
      homeResourceHandler.setDirectoriesListed(false);
      homeResourceHandler.setWelcomeFiles(new String[]{"index.html"});

      ContextHandler homeContext = new ContextHandler();
      homeContext.setContextPath("/");
      homeContext.setResourceBase(homeWarDir.toString());
      homeContext.setHandler(homeResourceHandler);
      homeContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

      ResourceHandler instanceResourceHandler = new ResourceHandler();
      instanceResourceHandler.setResourceBase(instanceWarDir.toString());
      instanceResourceHandler.setDirectoriesListed(false);
      instanceResourceHandler.setWelcomeFiles(new String[]{"index.html"});

      ContextHandler instanceContext = new ContextHandler();
      instanceContext.setContextPath("/");
      instanceContext.setResourceBase(instanceWarDir.toString());
      instanceContext.setHandler(instanceResourceHandler);
      homeContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

      DefaultHandler defaultHandler = new DefaultHandler();
      defaultHandler.setServeIcon(false);

      handlers.addHandler(homeContext);
      handlers.addHandler(instanceContext);
      handlers.addHandler(defaultHandler);
      server.setHandler(handlers);
   }

   @Override
   public void start() throws Exception {
      if (isStarted()) {
         return;
      }
      server.start();
      ActiveMQWebLogger.LOGGER.webserverStarted(webServerConfig.bind);

      if (consoleUrl != null) {
         ActiveMQWebLogger.LOGGER.jolokiaAvailable(consoleUrl + "/jolokia");
         ActiveMQWebLogger.LOGGER.consoleAvailable(consoleUrl);
      }
   }

   public void internalStop() throws Exception {
      server.stop();
      if (webContexts != null) {
         File tmpdir = null;
         for (WebAppContext context : webContexts) {
            tmpdir = context.getTempDirectory();

            if (tmpdir != null && !context.isPersistTempDirectory()) {
               //tmpdir will be removed by deleteOnExit()
               //somehow when broker is stopped and restarted quickly
               //this tmpdir won't get deleted sometimes
               boolean fileDeleted = TimeUtils.waitOnBoolean(false, 5000, tmpdir::exists);

               if (!fileDeleted) {
                  //because the execution order of shutdown hooks are
                  //not determined, so it's possible that the deleteOnExit
                  //is executed after this hook, in that case we force a delete.
                  FileUtil.deleteDirectory(tmpdir);
                  logger.debug("Force to delete temporary file on shutdown: " + tmpdir.getAbsolutePath());
                  if (tmpdir.exists()) {
                     ActiveMQWebLogger.LOGGER.tmpFileNotDeleted(tmpdir);
                  }
               }
            }
         }
         webContexts.clear();
      }
   }

   @Override
   public boolean isStarted() {
      return server != null && server.isStarted();
   }

   /**
    * @return started server's port number; useful if it was specified as 0 (to use a random port)
    */
   public int getPort() {
      return (connector != null) ? connector.getLocalPort() : -1;
   }

   private WebAppContext deployWar(String url, String warFile, Path warDirectory) throws IOException {
      WebAppContext webapp = new WebAppContext();
      if (url.startsWith("/")) {
         webapp.setContextPath(url);
      } else {
         webapp.setContextPath("/" + url);
      }

      webapp.setWar(warDirectory.resolve(warFile).toString());
      handlers.addHandler(webapp);
      return webapp;
   }

   @Override
   public void stop() throws Exception {
      stop(false);
   }

   @Override
   public void stop(boolean isShutdown) throws Exception {
      if (isShutdown) {
         internalStop();
      }
   }
}
