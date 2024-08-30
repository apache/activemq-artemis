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
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.ActiveMQWebLogger;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.components.ExternalComponent;
import org.apache.activemq.artemis.dto.AppDTO;
import org.apache.activemq.artemis.dto.BindingDTO;
import org.apache.activemq.artemis.dto.ComponentDTO;
import org.apache.activemq.artemis.dto.WebServerDTO;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.marker.WebServerComponentMarker;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.PemConfigUtil;
import org.eclipse.jetty.security.DefaultAuthenticatorFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.RequestLogWriter;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.Scanner;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport.checkPemProviderLoaded;

public class WebServerComponent implements ExternalComponent, WebServerComponentMarker {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   public static final String DIR_ALLOWED = "org.eclipse.jetty.servlet.Default.dirAllowed";

   // this should match the value of <display-name> in the console war's WEB-INF/web.xml
   public static final String WEB_CONSOLE_DISPLAY_NAME = System.getProperty("org.apache.activemq.artemis.webConsoleDisplayName", "hawtio");

   public static final boolean DEFAULT_SNI_HOST_CHECK_VALUE = true;

   public static final boolean DEFAULT_SNI_REQUIRED_VALUE = false;

   public static final boolean DEFAULT_SSL_AUTO_RELOAD_VALUE = false;

   public static final int DEFAULT_SCAN_PERIOD_VALUE = 5;

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

   private int scanPeriod;
   private Scanner scanner;
   private ScheduledExecutorScheduler scannerScheduler;
   private Map<String, List<Runnable>> scannerTasks = new HashMap<>();

   @Override
   public void configure(ComponentDTO config, String artemisInstance, String artemisHome) throws Exception {
      this.webServerConfig = (WebServerDTO) config;
      this.artemisInstance = artemisInstance;
      this.artemisHome = artemisHome;

      if (webServerConfig.getScanPeriod() != null) {
         scanPeriod = webServerConfig.getScanPeriod();
      } else {
         scanPeriod = DEFAULT_SCAN_PERIOD_VALUE;
      }

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

      server = new Server(new QueuedThreadPool(webServerConfig.maxThreads, webServerConfig.minThreads, webServerConfig.idleThreadTimeout));
      handlers = new HandlerList();

      HttpConfiguration httpConfiguration = new HttpConfiguration();

      if (webServerConfig.maxRequestHeaderSize != null) {
         httpConfiguration.setRequestHeaderSize(webServerConfig.maxRequestHeaderSize);
      }

      if (webServerConfig.maxResponseHeaderSize != null) {
         httpConfiguration.setResponseHeaderSize(webServerConfig.maxResponseHeaderSize);
      }

      if (this.webServerConfig.customizer != null) {
         try {
            httpConfiguration.addCustomizer((HttpConfiguration.Customizer) ClassloadingUtil.getInstanceWithTypeCheck(this.webServerConfig.customizer, HttpConfiguration.Customizer.class, this.getClass().getClassLoader()));
         } catch (Throwable t) {
            ActiveMQWebLogger.LOGGER.customizerNotLoaded(this.webServerConfig.customizer, t);
         }
      }

      List<BindingDTO> bindings = this.webServerConfig.getAllBindings();
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
               webContext.getSessionHandler().getSessionCookieConfig().setComment("__SAME_SITE_STRICT__");
               webContext.addEventListener(new ServletContextListener() {
                  @Override
                  public void contextInitialized(ServletContextEvent sce) {
                     sce.getServletContext().addListener(new ServletRequestListener() {
                        @Override
                        public void requestDestroyed(ServletRequestEvent sre) {
                           ServletRequestListener.super.requestDestroyed(sre);
                           AuditLogger.currentCaller.remove();
                           AuditLogger.remoteAddress.remove();
                        }
                     });
                  }
               });
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
      defaultHandler.setRootRedirectLocation(this.webServerConfig.rootRedirectLocation);

      if (this.webServerConfig.requestLog != null &&
         this.webServerConfig.requestLog.filename != null) {
         server.setRequestLog(getRequestLog());
      }

      if (this.webServerConfig.webContentEnabled != null &&
         this.webServerConfig.webContentEnabled) {
         handlers.addHandler(homeContext);
         handlers.addHandler(instanceContext);
      }

      handlers.addHandler(defaultHandler); // this should be last

      server.setHandler(handlers);

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
         if (binding.keyStoreType != null) {
            sslFactory.setKeyStoreType(binding.keyStoreType);
            checkPemProviderLoaded(binding.keyStoreType);
         }
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
               if (binding.trustStoreType != null) {
                  sslFactory.setTrustStoreType(binding.trustStoreType);
                  checkPemProviderLoaded(binding.trustStoreType);
               }
            }
         }
         if (Boolean.TRUE.equals(binding.getSslAutoReload())) {
            addStoreResourceScannerTask(binding.getKeyStorePath(), binding.getKeyStoreType(), sslFactory);
            addStoreResourceScannerTask(binding.getTrustStorePath(), binding.getTrustStoreType(), sslFactory);
         }

         SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslFactory, "HTTP/1.1");

         SecureRequestCustomizer secureRequestCustomizer = new SecureRequestCustomizer();
         secureRequestCustomizer.setSniHostCheck(binding.getSniHostCheck() != null ? binding.getSniHostCheck() : DEFAULT_SNI_HOST_CHECK_VALUE);
         secureRequestCustomizer.setSniRequired(binding.getSniRequired() != null ? binding.getSniRequired() : DEFAULT_SNI_REQUIRED_VALUE);
         httpConfiguration.addCustomizer(secureRequestCustomizer);
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

   private File getStoreFile(String storeFilename) {
      File storeFile = new File(storeFilename);
      if (!storeFile.exists())
         throw new IllegalArgumentException("Store file does not exist: " + storeFilename);
      if (storeFile.isDirectory())
         throw new IllegalArgumentException("Expected store file not directory: " + storeFilename);

      return storeFile;
   }

   private File getParentStoreFile(File storeFile) {
      File parentFile = storeFile.getParentFile();
      if (!parentFile.exists() || !parentFile.isDirectory())
         throw new IllegalArgumentException("Error obtaining store dir for " + storeFile);

      return parentFile;
   }

   private Scanner getScanner() {
      if (scannerScheduler == null) {
         scannerScheduler = new ScheduledExecutorScheduler("WebScannerScheduler", true, 1);
         server.addBean(scannerScheduler);
      }

      if (scanner == null) {
         scanner = new Scanner(scannerScheduler);
         scanner.setScanInterval(scanPeriod);
         scanner.setReportDirs(false);
         scanner.setReportExistingFilesOnStartup(false);
         scanner.setScanDepth(1);
         scanner.addListener((Scanner.BulkListener) filenames -> {
            for (String filename: filenames) {
               List<Runnable> tasks = scannerTasks.get(filename);
               if (tasks != null) {
                  tasks.forEach(t -> t.run());
               }
            }
         });
         server.addBean(scanner);
      }

      return scanner;
   }

   private void addScannerTask(File file, Runnable task) {
      File parentFile = getParentStoreFile(file);
      String storeFilename = file.toPath().toString();
      List<Runnable> tasks = scannerTasks.get(storeFilename);
      if (tasks == null) {
         tasks = new ArrayList<>();
         scannerTasks.put(storeFilename, tasks);
      }
      tasks.add(task);
      getScanner().addDirectory(parentFile.toPath());
   }

   private void addStoreResourceScannerTask(String storeFilename, String storeType, SslContextFactory.Server sslFactory) {
      if (storeFilename != null) {
         File storeFile = getStoreFile(storeFilename);
         addScannerTask(storeFile, () -> {
            try {
               sslFactory.reload(f -> { });
            } catch (Exception e) {
               logger.warn("Failed to reload the ssl factory related to {}", storeFile, e);
            }
         });

         if (PemConfigUtil.isPemConfigStoreType(storeType)) {
            String[] sources;

            try (InputStream pemConfigStream = new FileInputStream(storeFile)) {
               sources = PemConfigUtil.parseSources(pemConfigStream);
            } catch (IOException e) {
               throw new IllegalArgumentException("Invalid PEM Config file: " + e);
            }

            if (sources != null) {
               for (String source : sources) {
                  addStoreResourceScannerTask(source, null, sslFactory);
               }
            }
         }

      }
   }

   private RequestLog getRequestLog() {
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

      return requestLog;
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

      String baseTempDir = temporaryWarDir.toFile().getAbsolutePath();
      webapp.setAttribute("org.eclipse.jetty.webapp.basetempdir", baseTempDir);
      webapp.setTempDirectory(new File(baseTempDir + File.separator + warFile));

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
         scanner = null;
         scannerScheduler = null;
         scannerTasks.clear();
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

   public Server getWebServer() {
      return server;
   }
}
