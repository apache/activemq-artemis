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

package org.apache.activemq.artemis.core.server;

import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.core.server.reload.ReloadCallback;
import org.jboss.logmanager.Configurator;
import org.jboss.logmanager.LogContext;
import org.jboss.logmanager.PropertyConfigurator;
import org.jboss.logmanager.config.LogContextConfiguration;

public class LoggingConfigurationFileReloader implements ReloadCallback {

   private final Lock lock = new ReentrantLock();
   private final org.jboss.logmanager.Logger.AttachmentKey<LoggingConfigurationUpdater> KEY = new org.jboss.logmanager.Logger.AttachmentKey<>();

   @Override
   public void reload(URL uri) throws Exception {
      ActiveMQServerLogger.LOGGER.reloadingConfiguration("logging");
      final LoggingConfigurationUpdater updater = getOrCreateUpdater();
      if (updater == null) {
         ActiveMQServerLogger.LOGGER.loggingReloadFailed(uri.toString(), null);
         return;
      }
      try (InputStream in = uri.openStream()) {
         lock.lock();
         updater.configure(in);
      } catch (Exception e) {
         ActiveMQServerLogger.LOGGER.loggingReloadFailed(uri.toString(), e);
      } finally {
         lock.unlock();
      }
   }

   private LoggingConfigurationUpdater getOrCreateUpdater() {
      final LogContext logContext = LogContext.getLogContext();
      final org.jboss.logmanager.Logger rootLogger = logContext.getLogger("");
      LoggingConfigurationUpdater updater = rootLogger.getAttachment(KEY);
      if (updater == null) {
         final LogContextConfiguration logContextConfiguration = getOrCreateConfiguration(rootLogger);
         if (logContextConfiguration == null) {
            return null;
         }
         updater = new LoggingConfigurationUpdater(logContextConfiguration);
         final LoggingConfigurationUpdater appearing = rootLogger.attachIfAbsent(KEY, updater);
         if (appearing != null) {
            updater = appearing;
         }
      }
      return updater;
   }

   private LogContextConfiguration getOrCreateConfiguration(final org.jboss.logmanager.Logger rootLogger) {
      Configurator configurator = rootLogger.getAttachment(Configurator.ATTACHMENT_KEY);
      if (configurator == null) {
         configurator = new PropertyConfigurator(rootLogger.getLogContext());
         final Configurator appearing = rootLogger.attachIfAbsent(Configurator.ATTACHMENT_KEY, configurator);
         if (appearing != null) {
            configurator = appearing;
         }
      }
      if (configurator instanceof PropertyConfigurator) {
         return ((PropertyConfigurator) configurator).getLogContextConfiguration();
      }
      if (configurator instanceof LogContextConfiguration) {
         return (LogContextConfiguration) configurator;
      }
      return null;
   }
}