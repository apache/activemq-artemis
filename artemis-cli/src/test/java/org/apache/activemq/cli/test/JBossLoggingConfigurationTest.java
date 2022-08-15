/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.cli.test;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Properties;

import org.apache.activemq.artemis.logs.JBossLoggingConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JBossLoggingConfigurationTest {

   private JBossLoggingConfiguration config;

   @Before
   public void setUp() {
      config = JBossLoggingConfiguration.createDefaultArtemisLoggingProperties();
   }

   @Test
   public void testDefaultConfig() throws Exception {
      StringWriter stringWriter = new StringWriter();
      config.save(new PrintWriter(stringWriter));

      StringReader reader = new StringReader(stringWriter.toString());
      Properties loggingProperties = new Properties();
      loggingProperties.load(reader);

      String defaultLogging = "org/apache/activemq/artemis/cli/commands/etc/logging.properties";
      URL defaultUrl = getClass().getClassLoader().getResource(defaultLogging);
      Properties defaultLoggingProperties = new Properties();
      defaultLoggingProperties.load(defaultUrl.openStream());

      Assert.assertNotEquals(loggingProperties, defaultLoggingProperties);
      Assert.assertEquals(loggingProperties.get("logger.handlers"), "CONSOLE");
      Assert.assertEquals(loggingProperties.get("logger.org.apache.activemq.audit.base.handlers"), "CONSOLE");
      Assert.assertEquals(loggingProperties.get("logger.org.apache.activemq.audit.resource.handlers"), "CONSOLE");
      Assert.assertEquals(loggingProperties.get("logger.org.apache.activemq.audit.message.handlers"), "CONSOLE");
   }

   @Test
   public void testUpdateLoggingProperties() throws Exception {
      String loggerPropFile = "logger.properties";
      URL defaultUrl = getClass().getClassLoader().getResource(loggerPropFile);
      Properties properties = new Properties();
      properties.load(defaultUrl.openStream());
      config.updateFromProperties(properties);

      StringWriter stringWriter = new StringWriter();
      config.save(new PrintWriter(stringWriter));

      StringReader reader = new StringReader(stringWriter.toString());
      Properties updated = new Properties();
      updated.load(reader);

      Assert.assertEquals(updated.get("logger.handlers"), "CONSOLE");
      Assert.assertEquals(updated.get("logger.org.apache.activemq.audit.base.handlers"), "CONSOLE");
      Assert.assertEquals(updated.get("logger.org.apache.activemq.audit.base.level"), "WARN");
      Assert.assertEquals(updated.get("logger.level"), "WARN");
      Assert.assertEquals(updated.get("handler.CONSOLE.formatter"), "PATTERN2");
      Assert.assertEquals(updated.get("handler.FILE"), "org.jboss.logmanager.handlers.SomeOtherHandler");
      Assert.assertEquals(updated.get("handler.FILE.level"), "ERROR");
      Assert.assertEquals(updated.get("handler.FILE.properties"), "suffix2,append2,autoFlush2,fileName2");
      Assert.assertEquals(updated.get("handler.FILE.suffix2"), ".MM-dd");
      Assert.assertEquals(updated.get("handler.FILE.append2"), "true");
      Assert.assertEquals(updated.get("handler.FILE.autoFlush2"), "true");
      Assert.assertEquals(updated.get("handler.FILE.fileName2"), "${artemis.instance}/log/artemis.log");
      Assert.assertEquals(updated.get("handler.FILE.formatter"), "PATTERN2");

      Assert.assertEquals(updated.get("formatter.PATTERN"), "org.jboss.logmanager.formatters.SomeFormatter");
      Assert.assertEquals(updated.get("formatter.PATTERN.properties"), "patternx,debug");
      Assert.assertEquals(updated.get("formatter.PATTERN.patternx"), "%d %-5p [[%c]] %s%E%n");
      Assert.assertEquals(updated.get("formatter.PATTERN.debug"), "true");
   }

   @Test
   public void testAddingNewLogger() throws Exception {
      String newLoggerName = "org.apache.activemq.newlogger";
      Properties properties = new Properties();
      properties.put("logger." + newLoggerName + ".level", "DEBUG");
      properties.put("logger." + newLoggerName + ".handlers", "CONSOLE,FILE");
      properties.put("logger." + newLoggerName + ".useParentHandlers", "false");

      config.updateFromProperties(properties);

      StringWriter stringWriter = new StringWriter();
      config.save(new PrintWriter(stringWriter));

      StringReader reader = new StringReader(stringWriter.toString());
      Properties updated = new Properties();
      updated.load(reader);

      Assert.assertEquals(updated.get("logger." + newLoggerName + ".level"), "DEBUG");
      Assert.assertEquals(updated.get("logger." + newLoggerName + ".handlers"), "CONSOLE,FILE");
      Assert.assertEquals(updated.get("logger." + newLoggerName + ".useParentHandlers"), "false");

      Assert.assertTrue(((String)updated.get("loggers")).contains(newLoggerName));
   }

   @Test
   public void testAddingNewHandler() throws Exception {
      String newHandlerName = "NEW_HANDLER";
      Properties properties = new Properties();
      properties.put("handler." + newHandlerName + ".level", "DEBUG");
      properties.put("handler." + newHandlerName, "org.jboss.logmanager.handlers.NewHandler");
      properties.put("handler." + newHandlerName + ".properties", "prop1,prop2,prop3");
      properties.put("handler." + newHandlerName + ".prop1", "300");
      properties.put("handler." + newHandlerName + ".prop2", "value2");
      properties.put("handler." + newHandlerName + ".prop3", "500M");
      properties.put("handler." + newHandlerName + ".formatter", "NEW_FORMATTER");

      config.updateFromProperties(properties);

      StringWriter stringWriter = new StringWriter();
      config.save(new PrintWriter(stringWriter));

      StringReader reader = new StringReader(stringWriter.toString());
      Properties updated = new Properties();
      updated.load(reader);

      Assert.assertEquals(updated.get("handler." + newHandlerName + ".level"), "DEBUG");
      Assert.assertEquals(updated.get("handler." + newHandlerName), "org.jboss.logmanager.handlers.NewHandler");
      Assert.assertEquals(updated.get("handler." + newHandlerName + ".properties"), "prop1,prop2,prop3");
      Assert.assertEquals(updated.get("handler." + newHandlerName + ".prop1"), "300");
      Assert.assertEquals(updated.get("handler." + newHandlerName + ".prop2"), "value2");
      Assert.assertEquals(updated.get("handler." + newHandlerName + ".prop3"), "500M");
      Assert.assertEquals(updated.get("handler." + newHandlerName + ".formatter"), "NEW_FORMATTER");
   }

   @Test
   public void testAddingNewFormatter() throws Exception {
      String newFormatterName = "NEW_FORMATTER";
      Properties properties = new Properties();
      properties.put("formatter." + newFormatterName, "org.jboss.logmanager.formatters.NewFormatter");
      properties.put("formatter." + newFormatterName + ".properties", "prop1,prop2,prop3,pattern");
      properties.put("formatter." + newFormatterName + ".prop1", "300");
      properties.put("formatter." + newFormatterName + ".prop2", "value2");
      properties.put("formatter." + newFormatterName + ".prop3", "500M");
      properties.put("formatter." + newFormatterName + ".pattern", "%d [NEW_FORMATTER](%t) %s%E%n");

      config.updateFromProperties(properties);

      StringWriter stringWriter = new StringWriter();
      config.save(new PrintWriter(stringWriter));

      StringReader reader = new StringReader(stringWriter.toString());
      Properties updated = new Properties();
      updated.load(reader);

      Assert.assertEquals(updated.get("formatter." + newFormatterName), "org.jboss.logmanager.formatters.NewFormatter");
      Assert.assertEquals(updated.get("formatter." + newFormatterName + ".properties"), "prop1,prop2,prop3,pattern");
      Assert.assertEquals(updated.get("formatter." + newFormatterName + ".prop1"), "300");
      Assert.assertEquals(updated.get("formatter." + newFormatterName + ".prop2"), "value2");
      Assert.assertEquals(updated.get("formatter." + newFormatterName + ".prop3"), "500M");
      Assert.assertEquals(updated.get("formatter." + newFormatterName + ".pattern"), "%d [NEW_FORMATTER](%t) %s%E%n");
   }

}
