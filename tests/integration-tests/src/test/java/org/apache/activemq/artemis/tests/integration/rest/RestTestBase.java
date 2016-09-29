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
package org.apache.activemq.artemis.tests.integration.rest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import shaded.org.apache.commons.io.FileUtils;

public class RestTestBase extends JMSTestBase {

   @Rule
   public TemporaryFolder testFolder = new TemporaryFolder();

   protected Server server;
   protected File webAppDir;
   protected HandlerList handlers;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      webAppDir = testFolder.newFolder("test-apps");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      if (server != null) {
         try {
            server.stop();
         } catch (Throwable t) {
            t.printStackTrace();
         }
      }
      super.tearDown();
   }

   public Server createJettyServer(String host, int port) throws Exception {
      server = new Server();
      ServerConnector connector = new ServerConnector(server);
      connector.setHost(host);
      connector.setPort(port);
      server.setConnectors(new Connector[]{connector});

      handlers = new HandlerList();

      server.setHandler(handlers);
      return server;
   }

   public WebAppContext deployWebApp(String contextPath, File warFile) {
      WebAppContext webapp = new WebAppContext();
      if (contextPath.startsWith("/")) {
         webapp.setContextPath(contextPath);
      } else {
         webapp.setContextPath("/" + contextPath);
      }
      webapp.setWar(warFile.getAbsolutePath());

      handlers.addHandler(webapp);
      return webapp;
   }

   public File getResourceFile(String resPath, String warName) throws IOException {
      InputStream input = RestTestBase.class.getResourceAsStream(resPath);
      File result = new File(webAppDir, warName);
      FileUtils.copyInputStreamToFile(input, result);
      return result;
   }

}
