/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.reload;

import java.util.Collections;
import java.util.List;
import java.util.logging.LogManager;

import org.apache.activemq.artemis.core.server.LoggingConfigurationFileReloader;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.jboss.logging.Logger;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LoggingConfigurationFileReloaderTest {

   @Test
   public void test() throws Exception {
      LogManager logManager = LogManager.getLogManager();

      List loggerNames = Collections.list(logManager.getLoggerNames());
      assertFalse(loggerNames.contains("test1"));
      assertFalse(loggerNames.contains("test2"));
      assertFalse(loggerNames.contains("test3"));

      // everything defaults to INFO
      final Logger root = Logger.getLogger("");
      assertTrue(root.isEnabled(Logger.Level.ERROR));
      assertTrue(root.isEnabled(Logger.Level.WARN));
      assertTrue(root.isEnabled(Logger.Level.INFO));
      assertFalse(root.isEnabled(Logger.Level.DEBUG));
      assertFalse(root.isEnabled(Logger.Level.TRACE));

      final Logger test1 = Logger.getLogger("test1");
      assertTrue(test1.isEnabled(Logger.Level.ERROR));
      assertTrue(test1.isEnabled(Logger.Level.WARN));
      assertTrue(test1.isEnabled(Logger.Level.INFO));
      assertFalse(test1.isEnabled(Logger.Level.DEBUG));
      assertFalse(test1.isEnabled(Logger.Level.TRACE));

      final Logger test2 = Logger.getLogger("test2");
      assertTrue(test2.isEnabled(Logger.Level.ERROR));
      assertTrue(test2.isEnabled(Logger.Level.WARN));
      assertTrue(test2.isEnabled(Logger.Level.INFO));
      assertFalse(test2.isEnabled(Logger.Level.DEBUG));
      assertFalse(test2.isEnabled(Logger.Level.TRACE));

      LoggingConfigurationFileReloader loggingConfigurationFileReloader = new LoggingConfigurationFileReloader();
      loggingConfigurationFileReloader.reload(ClassloadingUtil.findResource("reload-logging-1.properties"));

      loggerNames = Collections.list(logManager.getLoggerNames());
      assertTrue(loggerNames.contains("test1"));
      assertTrue(loggerNames.contains("test2"));
      assertFalse(loggerNames.contains("test3"));

      assertTrue(root.isEnabled(Logger.Level.ERROR));
      assertTrue(root.isEnabled(Logger.Level.WARN));
      assertFalse(root.isEnabled(Logger.Level.INFO));
      assertFalse(root.isEnabled(Logger.Level.DEBUG));
      assertFalse(root.isEnabled(Logger.Level.TRACE));

      assertTrue(test1.isEnabled(Logger.Level.ERROR));
      assertTrue(test1.isEnabled(Logger.Level.WARN));
      assertTrue(test1.isEnabled(Logger.Level.INFO));
      assertTrue(test1.isEnabled(Logger.Level.DEBUG));
      assertTrue(test1.isEnabled(Logger.Level.TRACE));

      assertTrue(test2.isEnabled(Logger.Level.ERROR));
      assertFalse(test2.isEnabled(Logger.Level.WARN));
      assertFalse(test2.isEnabled(Logger.Level.INFO));
      assertFalse(test2.isEnabled(Logger.Level.DEBUG));
      assertFalse(test2.isEnabled(Logger.Level.TRACE));

      loggingConfigurationFileReloader.reload(ClassloadingUtil.findResource("reload-logging-2.properties"));

      loggerNames = Collections.list(logManager.getLoggerNames());
      assertTrue(loggerNames.contains("test1"));
      assertTrue(loggerNames.contains("test2"));
      assertTrue(loggerNames.contains("test3"));

      assertTrue(root.isEnabled(Logger.Level.ERROR));
      assertFalse(root.isEnabled(Logger.Level.WARN));
      assertFalse(root.isEnabled(Logger.Level.INFO));
      assertFalse(root.isEnabled(Logger.Level.DEBUG));
      assertFalse(root.isEnabled(Logger.Level.TRACE));

      assertTrue(test1.isEnabled(Logger.Level.ERROR));
      assertTrue(test1.isEnabled(Logger.Level.WARN));
      assertFalse(test1.isEnabled(Logger.Level.INFO));
      assertFalse(test1.isEnabled(Logger.Level.DEBUG));
      assertFalse(test1.isEnabled(Logger.Level.TRACE));

      final Logger test3 = Logger.getLogger("test3");
      assertTrue(test3.isEnabled(Logger.Level.ERROR));
      assertTrue(test3.isEnabled(Logger.Level.WARN));
      assertTrue(test3.isEnabled(Logger.Level.INFO));
      assertTrue(test3.isEnabled(Logger.Level.DEBUG));
      assertFalse(test3.isEnabled(Logger.Level.TRACE));
   }
}