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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

import org.apache.activemq.artemis.ActiveMQWebLogger;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jboss.logging.Logger;

import javax.servlet.DispatcherType;

public class WebServerComponent implements ExternalComponent {

   private static final Logger logger = Logger.getLogger(WebServerComponent.class);

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;
   private URI uri;
   private String consoleUrl;
   private List<WebAppContext> webContexts;
   private ServerConnector connector;
   private Path artemisHomePath;
   private Path temporaryWarDir;

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      webServerConfig = (WebServerDTO) config;
      uri = new URI(webServerConfig.bind);
      server = new Server();
      String scheme = uri.getScheme();

      HttpConfiguration httpConfiguration = new HttpConfiguration();

      if (webServerConfig.customizer != null) {
         try {
            httpConfiguration.addCustomizer((HttpConfiguration.Customizer) Class.forName(webServerConfig.customizer).getConstructor().newInstance());
         } catch (Throwable t) {
            ActiveMQWebLogger.LOGGER.customizerNotLoaded(webServerConfig.customizer, t);
         }
      }

      if ("https".equals(scheme)) {
         SslContextFactory.Server sslFactory = new SslContextFactory.Server();
         sslFactory.setKeyStorePath(webServerConfig.keyStorePath == null ? artemisInstance + "/etc/keystore.jks" : webServerConfig.keyStorePath);
         sslFactory.setKeyStorePassword(webServerConfig.getKeyStorePassword() == null ? "password" : webServerConfig.getKeyStorePassword());
         String[] ips = sslFactory.getIncludeProtocols();

         if (webServerConfig.getIncludedTLSProtocols() != null) {
            sslFactory.setIncludeProtocols(webServerConfig.getIncludedTLSProtocols());
         }
         if (webServerConfig.getExcludedTLSProtocols() != null) {
            sslFactory.setExcludeProtocols(webServerConfig.getExcludedTLSProtocols());
         }
         if (webServerConfig.getIncludedCipherSuites() != null) {
            sslFactory.setIncludeCipherSuites(webServerConfig.getIncludedCipherSuites());
         }
         if (webServerConfig.getExcludedCipherSuites() != null) {
            sslFactory.setExcludeCipherSuites(webServerConfig.getExcludedCipherSuites());
         }
         if (webServerConfig.clientAuth != null) {
            sslFactory.setNeedClientAuth(webServerConfig.clientAuth);
            if (webServerConfig.clientAuth) {
               sslFactory.setTrustStorePath(webServerConfig.trustStorePath);
               sslFactory.setTrustStorePassword(webServerConfig.getTrustStorePassword());
            }
         }

         SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslFactory, "HTTP/1.1");

         httpConfiguration.addCustomizer(new SecureRequestCustomizer());
         httpConfiguration.setSendServerVersion(false);
         HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfiguration);

         connector = new ServerConnector(server, sslConnectionFactory, httpFactory);

      } else {
         httpConfiguration.setSendServerVersion(false);
         ConnectionFactory connectionFactory = new HttpConnectionFactory(httpConfiguration);
         connector = new ServerConnector(server, connectionFactory);
      }
      connector.setPort(uri.getPort());
      connector.setHost(uri.getHost());

      server.setConnectors(new Connector[]{connector});

      handlers = new HandlerList();

      this.artemisHomePath = Paths.get(artemisHome != null ? artemisHome : ".");
      Path homeWarDir = artemisHomePath.resolve(webServerConfig.path).toAbsolutePath();
      Path instanceWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve(webServerConfig.path).toAbsolutePath();

      temporaryWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve("tmp").resolve("webapps").toAbsolutePath();
      if (!Files.exists(temporaryWarDir)) {
         Files.createDirectories(temporaryWarDir);
      }

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

      if (webServerConfig.requestLog != null) {
         handlers.addHandler(getLogHandler());
      }
      handlers.addHandler(homeContext);
      handlers.addHandler(instanceContext);
      handlers.addHandler(defaultHandler); // this should be last

      server.setHandler(handlers);
   }

   private RequestLogHandler getLogHandler() {
      RequestLogHandler requestLogHandler = new RequestLogHandler();
      NCSARequestLog requestLog = new NCSARequestLog();

      // required via config so no check necessary
      requestLog.setFilename(webServerConfig.requestLog.filename);

      if (webServerConfig.requestLog.append != null) {
         requestLog.setAppend(webServerConfig.requestLog.append);
      }

      if (webServerConfig.requestLog.extended != null) {
         requestLog.setExtended(webServerConfig.requestLog.extended);
      }

      if (webServerConfig.requestLog.logCookies != null) {
         requestLog.setLogCookies(webServerConfig.requestLog.logCookies);
      }

      if (webServerConfig.requestLog.logTimeZone != null) {
         requestLog.setLogTimeZone(webServerConfig.requestLog.logTimeZone);
      }

      if (webServerConfig.requestLog.filenameDateFormat != null) {
         requestLog.setFilenameDateFormat(webServerConfig.requestLog.filenameDateFormat);
      }

      if (webServerConfig.requestLog.retainDays != null) {
         requestLog.setRetainDays(webServerConfig.requestLog.retainDays);
      }

      if (webServerConfig.requestLog.ignorePaths != null && webServerConfig.requestLog.ignorePaths.length() > 0) {
         String[] split = webServerConfig.requestLog.ignorePaths.split(",");
         String[] ignorePaths = new String[split.length];
         for (int i = 0; i < ignorePaths.length; i++) {
            ignorePaths[i] = split[i].trim();
         }
         requestLog.setIgnorePaths(ignorePaths);
      }

      if (webServerConfig.requestLog.logDateFormat != null) {
         requestLog.setLogDateFormat(webServerConfig.requestLog.logDateFormat);
      }

      if (webServerConfig.requestLog.logLocale != null) {
         requestLog.setLogLocale(Locale.forLanguageTag(webServerConfig.requestLog.logLocale));
      }

      if (webServerConfig.requestLog.logLatency != null) {
         requestLog.setLogLatency(webServerConfig.requestLog.logLatency);
      }

      if (webServerConfig.requestLog.logServer != null) {
         requestLog.setLogServer(webServerConfig.requestLog.logServer);
      }

      if (webServerConfig.requestLog.preferProxiedForAddress != null) {
         requestLog.setPreferProxiedForAddress(webServerConfig.requestLog.preferProxiedForAddress);
      }

      requestLogHandler.setRequestLog(requestLog);

      return requestLogHandler;
   }

   @Override
   public void start() throws Exception {
      if (isStarted()) {
         return;
      }
      cleanupTmp();
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
         cleanupWebTemporaryFiles(webContexts);

         webContexts.clear();
      }
   }

   private File getLibFolder() {
      Path lib = artemisHomePath.resolve("lib");
      File libFolder = new File(lib.toUri());
      return libFolder;
   }

   private void cleanupTmp() {
      if (webContexts == null || webContexts.size() == 0) {
         //there is no webapp to be deployed (as in some tests)
         return;
      }

      try {
         List<File> temporaryFiles = new ArrayList<>();
         Files.newDirectoryStream(temporaryWarDir).forEach(path -> temporaryFiles.add(path.toFile()));

         if (temporaryFiles.size() > 0) {
            WebTmpCleaner.cleanupTmpFiles(getLibFolder(), temporaryFiles, true);
         }
      } catch (Exception e) {
         logger.warn("Failed to get base dir for tmp web files", e);
      }
   }

   public void cleanupWebTemporaryFiles(List<WebAppContext> webContexts) throws Exception {
      List<File> temporaryFiles = new ArrayList<>();
      for (WebAppContext context : webContexts) {
         File tmpdir = context.getTempDirectory();
         temporaryFiles.add(tmpdir);
      }

      if (!temporaryFiles.isEmpty()) {
         WebTmpCleaner.cleanupTmpFiles(getLibFolder(), temporaryFiles);
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
      //add the filters needed for audit logging
      webapp.addFilter(new FilterHolder(JolokiaFilter.class), "/*", EnumSet.of(DispatcherType.INCLUDE, DispatcherType.REQUEST));
      webapp.addFilter(new FilterHolder(AuthenticationFilter.class), "/auth/login/*", EnumSet.of(DispatcherType.REQUEST));

      webapp.setWar(warDirectory.resolve(warFile).toString());

      webapp.setAttribute("org.eclipse.jetty.webapp.basetempdir", temporaryWarDir.toFile().getAbsolutePath());

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

   public List<WebAppContext> getWebContexts() {
      return this.webContexts;
   }
}
