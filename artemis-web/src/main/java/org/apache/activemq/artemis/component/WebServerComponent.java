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

import javax.servlet.DispatcherType;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.ActiveMQWebLogger;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.BindingDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.eclipse.jetty.security.DefaultAuthenticatorFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.RequestLogWriter;
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

public class WebServerComponent implements ExternalComponent {

   private static final Logger logger = Logger.getLogger(WebServerComponent.class);

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;
   private final List<String> consoleUrls = new ArrayList<>();
   private final List<String> jolokiaUrls = new ArrayList<>();
   private List<WebAppContext> webContexts;
   private ServerConnector[] connectors;
   private Path artemisHomePath;
   private Path temporaryWarDir;

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      webServerConfig = (WebServerDTO) config;
      server = new Server();

      HttpConfiguration httpConfiguration = new HttpConfiguration();

      if (webServerConfig.customizer != null) {
         try {
            httpConfiguration.addCustomizer((HttpConfiguration.Customizer) Class.forName(webServerConfig.customizer).getConstructor().newInstance());
         } catch (Throwable t) {
            ActiveMQWebLogger.LOGGER.customizerNotLoaded(webServerConfig.customizer, t);
         }
      }

      List<BindingDTO> bindings = webServerConfig.getBindings();
      connectors = new ServerConnector[bindings.size()];
      String[] virtualHosts = new String[bindings.size()];

      for (int i = 0; i < bindings.size(); i++) {
         BindingDTO binding = bindings.get(i);
         URI uri = new URI(binding.uri);
         String scheme = uri.getScheme();
         ServerConnector connector;

         if ("https".equals(scheme)) {
            SslContextFactory.Server sslFactory = new SslContextFactory.Server();
            sslFactory.setKeyStorePath(binding.keyStorePath == null ? artemisInstance + "/etc/keystore.jks" : binding.keyStorePath);
            sslFactory.setKeyStorePassword(binding.getKeyStorePassword() == null ? "password" : binding.getKeyStorePassword());

            if (binding.getIncludedTLSProtocols() != null) {
               sslFactory.setIncludeProtocols(binding.getIncludedTLSProtocols());
            }
            if (binding.getExcludedTLSProtocols() != null) {
               sslFactory.setExcludeProtocols(binding.getExcludedTLSProtocols());
            }
            if (binding.getIncludedCipherSuites() != null) {
               sslFactory.setIncludeCipherSuites(binding.getIncludedCipherSuites());
            }
            if (binding.getExcludedCipherSuites() != null) {
               sslFactory.setExcludeCipherSuites(binding.getExcludedCipherSuites());
            }
            if (binding.clientAuth != null) {
               sslFactory.setNeedClientAuth(binding.clientAuth);
               if (binding.clientAuth) {
                  sslFactory.setTrustStorePath(binding.trustStorePath);
                  sslFactory.setTrustStorePassword(binding.getTrustStorePassword());
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
         connector.setName("Connector-" + i);

         connectors[i] = connector;
         virtualHosts[i] = "@Connector-" + i;
      }

      server.setConnectors(connectors);

      handlers = new HandlerList();

      this.artemisHomePath = Paths.get(artemisHome != null ? artemisHome : ".");
      Path homeWarDir = artemisHomePath.resolve(webServerConfig.path).toAbsolutePath();
      Path instanceWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve(webServerConfig.path).toAbsolutePath();

      temporaryWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve("tmp").resolve("webapps").toAbsolutePath();
      if (!Files.exists(temporaryWarDir)) {
         Files.createDirectories(temporaryWarDir);
      }

      for (int i = 0; i < bindings.size(); i++) {
         BindingDTO binding = bindings.get(i);
         if (binding.apps != null && binding.apps.size() > 0) {
            webContexts = new ArrayList<>();
            for (AppDTO app : binding.apps) {
               Path dirToUse = homeWarDir;
               if (new File(instanceWarDir.toFile().toString() + File.separator + app.war).exists()) {
                  dirToUse = instanceWarDir;
               }
               WebAppContext webContext = deployWar(app.url, app.war, dirToUse, virtualHosts[i]);
               webContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
               webContexts.add(webContext);
               if (app.war.startsWith("console")) {
                  consoleUrls.add(binding.uri + "/" + app.url);
                  jolokiaUrls.add(binding.uri + "/" + app.url + "/jolokia");
               }
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
      homeContext.setVirtualHosts(virtualHosts);
      homeContext.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

      ResourceHandler instanceResourceHandler = new ResourceHandler();
      instanceResourceHandler.setResourceBase(instanceWarDir.toString());
      instanceResourceHandler.setDirectoriesListed(false);
      instanceResourceHandler.setWelcomeFiles(new String[]{"index.html"});

      ContextHandler instanceContext = new ContextHandler();
      instanceContext.setContextPath("/");
      instanceContext.setResourceBase(instanceWarDir.toString());
      instanceContext.setHandler(instanceResourceHandler);
      instanceContext.setVirtualHosts(virtualHosts);
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
      RequestLogWriter requestLogWriter = new RequestLogWriter();
      CustomRequestLog requestLog;

      // required via config so no check necessary
      requestLogWriter.setFilename(webServerConfig.requestLog.filename);

      if (webServerConfig.requestLog.append != null) {
         requestLogWriter.setAppend(webServerConfig.requestLog.append);
      }

      if (webServerConfig.requestLog.filenameDateFormat != null) {
         requestLogWriter.setFilenameDateFormat(webServerConfig.requestLog.filenameDateFormat);
      }

      if (webServerConfig.requestLog.retainDays != null) {
         requestLogWriter.setRetainDays(webServerConfig.requestLog.retainDays);
      }

      if (webServerConfig.requestLog.format != null) {
         requestLog = new CustomRequestLog(requestLogWriter, webServerConfig.requestLog.format);
      } else if (webServerConfig.requestLog.extended != null && webServerConfig.requestLog.extended) {
         requestLog = new CustomRequestLog(requestLogWriter, CustomRequestLog.EXTENDED_NCSA_FORMAT);
      } else {
         requestLog = new CustomRequestLog(requestLogWriter, CustomRequestLog.NCSA_FORMAT);
      }

      if (webServerConfig.requestLog.ignorePaths != null && webServerConfig.requestLog.ignorePaths.length() > 0) {
         String[] split = webServerConfig.requestLog.ignorePaths.split(",");
         String[] ignorePaths = new String[split.length];
         for (int i = 0; i < ignorePaths.length; i++) {
            ignorePaths[i] = split[i].trim();
         }
         requestLog.setIgnorePaths(ignorePaths);
      }

      RequestLogHandler requestLogHandler = new RequestLogHandler();
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

      String bindings = webServerConfig.getBindings()
            .stream()
            .map(binding -> binding.uri)
            .collect(Collectors.joining(", "));
      ActiveMQWebLogger.LOGGER.webserverStarted(bindings);

      ActiveMQWebLogger.LOGGER.jolokiaAvailable(String.join(", ", jolokiaUrls));
      ActiveMQWebLogger.LOGGER.consoleAvailable(String.join(", ", consoleUrls));
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
   @Deprecated
   public int getPort() {
      return getPort(0);
   }

   public int getPort(int connectorIndex) {
      if (connectorIndex < connectors.length) {
         return connectors[connectorIndex].getLocalPort();
      }
      return -1;
   }

   private WebAppContext deployWar(String url, String warFile, Path warDirectory, String virtualHost) {
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

      // Set the default authenticator factory to avoid NPE due to the following commit:
      // https://github.com/eclipse/jetty.project/commit/7e91d34177a880ecbe70009e8f200d02e3a0c5dd
      webapp.getSecurityHandler().setAuthenticatorFactory(new DefaultAuthenticatorFactory());

      webapp.setVirtualHosts(new String[]{virtualHost});

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
