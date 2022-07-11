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
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.BindingDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.apache.activemq.artemis.marker.WebServerComponentMarker;
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

public class WebServerComponent implements ExternalComponent, WebServerComponentMarker {

   private static final Logger logger = Logger.getLogger(WebServerComponent.class);
   public static final String DIR_ALLOWED = "org.eclipse.jetty.servlet.Default.dirAllowed";

   // this should match the value of <display-name> in the console war's WEB-INF/web.xml
   public static final String WEB_CONSOLE_DISPLAY_NAME = System.getProperty("org.apache.activemq.artemis.webConsoleDisplayName", "hawtio");

   private Server server;
   private HandlerList handlers;
   private WebServerDTO webServerConfig;
   private final List<String> consoleUrls = new ArrayList<>();
   private final List<String> jolokiaUrls = new ArrayList<>();
   private final List<Pair<WebAppContext, String>> webContextData = new ArrayList<>();
   private ServerConnector[] connectors;
   private Path artemisHomePath;
   private Path temporaryWarDir;
   private String artemisInstance;
   private String artemisHome;

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      this.webServerConfig = (WebServerDTO) config;
      this.artemisInstance = artemisInstance;
      this.artemisHome = artemisHome;

      temporaryWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve("tmp").resolve("webapps").toAbsolutePath();
      if (!Files.exists(temporaryWarDir)) {
         Files.createDirectories(temporaryWarDir);
      }
   }

   @Override
   public synchronized void start() throws Exception {
      if (isStarted()) {
         return;
      }
      ActiveMQWebLogger.LOGGER.startingEmbeddedWebServer();

      server = new Server();
      handlers = new HandlerList();

      HttpConfiguration httpConfiguration = new HttpConfiguration();

      if (this.webServerConfig.customizer != null) {
         try {
            httpConfiguration.addCustomizer((HttpConfiguration.Customizer) Class.forName(this.webServerConfig.customizer).getConstructor().newInstance());
         } catch (Throwable t) {
            ActiveMQWebLogger.LOGGER.customizerNotLoaded(this.webServerConfig.customizer, t);
         }
      }

      List<BindingDTO> bindings = this.webServerConfig.getBindings();
      connectors = new ServerConnector[bindings.size()];
      String[] virtualHosts = new String[bindings.size()];

      this.artemisHomePath = Paths.get(artemisHome != null ? artemisHome : ".");
      Path homeWarDir = artemisHomePath.resolve(this.webServerConfig.path).toAbsolutePath();
      Path instanceWarDir = Paths.get(artemisInstance != null ? artemisInstance : ".").resolve(this.webServerConfig.path).toAbsolutePath();

      for (int i = 0; i < bindings.size(); i++) {
         BindingDTO binding = bindings.get(i);
         URI uri = new URI(binding.uri);
         String scheme = uri.getScheme();
         ServerConnector connector = createServerConnector(httpConfiguration, i, binding, uri, scheme);

         connectors[i] = connector;
         virtualHosts[i] = "@Connector-" + i;

         if (binding.apps != null && binding.apps.size() > 0) {
            for (AppDTO app : binding.apps) {
               Path dirToUse = homeWarDir;
               if (new File(instanceWarDir.toFile() + File.separator + app.war).exists()) {
                  dirToUse = instanceWarDir;
               }
               WebAppContext webContext = createWebAppContext(app.url, app.war, dirToUse, virtualHosts[i]);
               handlers.addHandler(webContext);
               webContext.setInitParameter(DIR_ALLOWED, "false");
               webContextData.add(new Pair(webContext, binding.uri));
            }
         }
      }

      server.setConnectors(connectors);

      ResourceHandler homeResourceHandler = new ResourceHandler();
      homeResourceHandler.setResourceBase(homeWarDir.toString());
      homeResourceHandler.setDirectoriesListed(false);
      homeResourceHandler.setWelcomeFiles(new String[]{"index.html"});

      ContextHandler homeContext = new ContextHandler();
      homeContext.setContextPath("/");
      homeContext.setResourceBase(homeWarDir.toString());
      homeContext.setHandler(homeResourceHandler);
      homeContext.setVirtualHosts(virtualHosts);
      homeContext.setInitParameter(DIR_ALLOWED, "false");

      ResourceHandler instanceResourceHandler = new ResourceHandler();
      instanceResourceHandler.setResourceBase(instanceWarDir.toString());
      instanceResourceHandler.setDirectoriesListed(false);
      instanceResourceHandler.setWelcomeFiles(new String[]{"index.html"});

      ContextHandler instanceContext = new ContextHandler();
      instanceContext.setContextPath("/");
      instanceContext.setResourceBase(instanceWarDir.toString());
      instanceContext.setHandler(instanceResourceHandler);
      instanceContext.setVirtualHosts(virtualHosts);
      homeContext.setInitParameter(DIR_ALLOWED, "false");

      DefaultHandler defaultHandler = new DefaultHandler();
      defaultHandler.setServeIcon(false);

      if (this.webServerConfig.requestLog != null) {
         handlers.addHandler(getLogHandler());
      }
      handlers.addHandler(homeContext);
      handlers.addHandler(instanceContext);
      handlers.addHandler(defaultHandler); // this should be last

      server.setHandler(handlers);

      cleanupTmp();
      server.start();

      printStatus(bindings);
   }

   private void printStatus(List<BindingDTO> bindings) {
      ActiveMQWebLogger.LOGGER.webserverStarted(bindings
                                                   .stream()
                                                   .map(binding -> binding.uri)
                                                   .collect(Collectors.joining(", ")));

      // the web server has to start before the war's web.xml will be parsed
      for (Pair<WebAppContext, String> data : webContextData) {
         if (WEB_CONSOLE_DISPLAY_NAME.equals(data.getA().getDisplayName())) {
            consoleUrls.add(data.getB() + data.getA().getContextPath());
            jolokiaUrls.add(data.getB() + data.getA().getContextPath() + "/jolokia");
         }
      }
      if (!jolokiaUrls.isEmpty()) {
         ActiveMQWebLogger.LOGGER.jolokiaAvailable(String.join(", ", jolokiaUrls));
      }
      if (!consoleUrls.isEmpty()) {
         ActiveMQWebLogger.LOGGER.consoleAvailable(String.join(", ", consoleUrls));
      }
   }

   private ServerConnector createServerConnector(HttpConfiguration httpConfiguration,
                                              int i,
                                              BindingDTO binding,
                                              URI uri,
                                              String scheme) throws Exception {
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
      return connector;
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

   private File getLibFolder() {
      Path lib = artemisHomePath.resolve("lib");
      File libFolder = new File(lib.toUri());
      return libFolder;
   }

   private void cleanupTmp() {
      if (webContextData.size() == 0) {
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

   public void cleanupWebTemporaryFiles(List<Pair<WebAppContext, String>> webContextData) throws Exception {
      List<File> temporaryFiles = new ArrayList<>();
      for (Pair<WebAppContext, String> data : webContextData) {
         File tmpdir = data.getA().getTempDirectory();
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

   protected WebAppContext createWebAppContext(String url, String warFile, Path warDirectory, String virtualHost) {
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

      return webapp;
   }

   @Override
   public void stop() throws Exception {
      stop(false);
   }

   @Override
   public synchronized void stop(boolean isShutdown) throws Exception {
      if (isShutdown && isStarted()) {
         ActiveMQWebLogger.LOGGER.stoppingEmbeddedWebServer();
         server.stop();
         server = null;
         cleanupWebTemporaryFiles(webContextData);
         webContextData.clear();
         jolokiaUrls.clear();
         consoleUrls.clear();
         handlers = null;
         ActiveMQWebLogger.LOGGER.stoppedEmbeddedWebServer();
      }
   }

   public List<Pair<WebAppContext, String>> getWebContextData() {
      return this.webContextData;
   }
}
