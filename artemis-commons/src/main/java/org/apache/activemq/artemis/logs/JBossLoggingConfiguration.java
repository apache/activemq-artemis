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

package org.apache.activemq.artemis.logs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * This class manages jboss-logging configuration file
 * in a ordered manner.
 */
public class JBossLoggingConfiguration {

   private HeaderSession headerSession = new HeaderSession();
   private LoggerNamesSection loggerNamesSection = new LoggerNamesSection();
   private LoggingSection loggingSection = new LoggingSection();
   private LoggerHandlerSection loggerHandlerSection = new LoggerHandlerSection();
   private LoggerFormatterSection loggerFormatterSection = new LoggerFormatterSection();

   public void save(PrintWriter fileWriter) throws IOException {
      headerSession.write(fileWriter);
      loggerNamesSection.write(fileWriter);
      loggingSection.write(fileWriter);
      loggerHandlerSection.write(fileWriter);
      loggerFormatterSection.write(fileWriter);
      fileWriter.flush();
   }

   // this gives comments a unique key in order to be put into a map
   public static String getCommentID() {
      return UUID.randomUUID().toString();
   }

   public static JBossLoggingConfiguration createDefaultArtemisLoggingProperties() {
      JBossLoggingConfiguration loggingConfig = new JBossLoggingConfiguration();
      loggingConfig.loggerNamesSection.addComment("Additional logger names to configure (root logger is always configured)");
      loggingConfig.loggerNamesSection.addComment("Root logger option");
      loggingConfig.loggerNamesSection.addLogger("org.eclipse.jetty");
      loggingConfig.loggerNamesSection.addLogger("org.jboss.logging");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.core.server");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.utils");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.utils.critical");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.journal");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.jms.server");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.artemis.integration.bootstrap");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.audit.base");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.audit.message");
      loggingConfig.loggerNamesSection.addLogger("org.apache.activemq.audit.resource");
      loggingConfig.loggerNamesSection.addLogger("org.apache.curator");
      loggingConfig.loggerNamesSection.addLogger("org.apache.zookeeper");

      loggingConfig.loggingSection.addComment("Root logger level");
      loggingConfig.loggingSection.addLogger("", "INFO");
      loggingConfig.loggingSection.addConfigEntry(getCommentID(), new ConfigEntry("ActiveMQ Artemis logger levels"));
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.core.server", "INFO");
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.journal", "INFO");
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.utils", "INFO");
      loggingConfig.loggingSection.addConfigEntry(getCommentID(), new ConfigEntry("if you have issues with CriticalAnalyzer, setting this as TRACE would give you extra troubleshooting information."));
      loggingConfig.loggingSection.addConfigEntry(getCommentID(), new ConfigEntry("but do not use it regularly as it would incur in some extra CPU usage for this diagnostic."));
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.utils.critical", "INFO");
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.jms", "INFO");
      loggingConfig.loggingSection.addLogger("org.apache.activemq.artemis.integration.bootstrap", "INFO");
      loggingConfig.loggingSection.addLogger("org.eclipse.jetty", "WARN");
      loggingConfig.loggingSection.addLoggerHandler("", "CONSOLE");

      loggingConfig.loggingSection.addConfigEntry(getCommentID(), new ConfigEntry("quorum logger levels"));
      loggingConfig.loggingSection.addLogger("org.apache.curator", "WARN");
      loggingConfig.loggingSection.addLogger("org.apache.zookeeper", "ERROR");
      loggingConfig.loggingSection.addConfigEntry(getCommentID(), new ConfigEntry("to enable audit change the level to INFO"));
      loggingConfig.loggingSection.addLogger("org.apache.activemq.audit.base", "ERROR");
      loggingConfig.loggingSection.addLoggerHandler("org.apache.activemq.audit.base", "CONSOLE");
      loggingConfig.loggingSection.addLoggerUseParentHandlersFlag("org.apache.activemq.audit.base", false);
      loggingConfig.loggingSection.addLogger("org.apache.activemq.audit.resource", "ERROR");
      loggingConfig.loggingSection.addLoggerHandler("org.apache.activemq.audit.resource", "CONSOLE");
      loggingConfig.loggingSection.addLoggerUseParentHandlersFlag("org.apache.activemq.audit.resource", false);
      loggingConfig.loggingSection.addLogger("org.apache.activemq.audit.message", "ERROR");
      loggingConfig.loggingSection.addLoggerHandler("org.apache.activemq.audit.message", "CONSOLE");
      loggingConfig.loggingSection.addLoggerUseParentHandlersFlag("org.apache.activemq.audit.message", false);

      loggingConfig.loggerHandlerSection.addConfigEntry(getCommentID(), new ConfigEntry("Console handler configuration"));
      List<String> propValues = new ArrayList<>();
      propValues.add("true");
      loggingConfig.loggerHandlerSection.addHandler("CONSOLE", "org.jboss.logmanager.handlers.ConsoleHandler",
                                                    "autoFlush", propValues, "DEBUG", "PATTERN");

      loggingConfig.loggerHandlerSection.addConfigEntry(getCommentID(), new ConfigEntry("File handler configuration"));
      propValues = new ArrayList<>();
      propValues.add(".yyyy-MM-dd");
      propValues.add("true");
      propValues.add("true");
      propValues.add("${artemis.instance}/log/artemis.log");

      loggingConfig.loggerHandlerSection.addHandler("FILE", "org.jboss.logmanager.handlers.PeriodicRotatingFileHandler",
                                                    "suffix,append,autoFlush,fileName",
                                                    propValues, "DEBUG", "PATTERN");

      loggingConfig.loggerHandlerSection.addConfigEntry(getCommentID(), new ConfigEntry("Audit logger"));
      propValues = new ArrayList<>();
      propValues.add(".yyyy-MM-dd");
      propValues.add("true");
      propValues.add("true");
      propValues.add("${artemis.instance}/log/audit.log");

      loggingConfig.loggerHandlerSection.addHandler("AUDIT_FILE", "org.jboss.logmanager.handlers.PeriodicRotatingFileHandler",
                                                    "suffix,append,autoFlush,fileName",
                                                    propValues, "INFO", "AUDIT_PATTERN");

      loggingConfig.loggerFormatterSection.addConfigEntry(getCommentID(), new ConfigEntry("Formatter pattern configuration"));
      propValues = new ArrayList<>();
      propValues.add("%d %-5p [%c] %s%E%n");
      loggingConfig.loggerFormatterSection.addFormatter("PATTERN", "org.jboss.logmanager.formatters.PatternFormatter",
                                                        "pattern", propValues);

      propValues = new ArrayList<>();
      propValues.add("%d [AUDIT](%t) %s%E%n");
      loggingConfig.loggerFormatterSection.addFormatter("AUDIT_PATTERN", "org.jboss.logmanager.formatters.PatternFormatter",
                                                        "pattern", propValues);

      return loggingConfig;
   }

   public void updateFromProperties(Properties logProps) {
      Iterator<Map.Entry<Object, Object>> iter = logProps.entrySet().iterator();
      while (iter.hasNext()) {
         Map.Entry<Object, Object> ent = iter.next();
         update((String)ent.getKey(), (String)ent.getValue());
      }
   }

   public void updateFrom(File loggerPropertiesFile) throws IOException {
      Properties logProps = new Properties();
      logProps.load(new FileReader(loggerPropertiesFile));
      updateFromProperties(logProps);
   }

   public void update(String key, String value) {
      if (key.equals("loggers")) {
         loggerNamesSection.updateLoggers(value);
      } else if (key.startsWith("logger.")) {
         String newLogger = loggingSection.updateLogger(key, value);
         if (newLogger != null) {
            loggerNamesSection.updateLoggers(newLogger);
         }
      } else if (key.startsWith("handler.")) {
         loggerHandlerSection.updateHandler(key, value);
      } else if (key.startsWith("formatter.")) {
         loggerFormatterSection.updateFormatter(key, value);
      }
   }

   private class HeaderSession {
      private String licenseText = "#\n" +
         "# Licensed to the Apache Software Foundation (ASF) under one or more\n" +
         "# contributor license agreements. See the NOTICE file distributed with\n" +
         "# this work for additional information regarding copyright ownership.\n" +
         "# The ASF licenses this file to You under the Apache License, Version 2.0\n" +
         "# (the \"License\"); you may not use this file except in compliance with\n" +
         "# the License. You may obtain a copy of the License at\n" +
         "#\n" +
         "#     http://www.apache.org/licenses/LICENSE-2.0\n" +
         "#\n" + "# Unless required by applicable law or agreed to in writing, software\n" +
         "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
         "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
         "# See the License for the specific language governing permissions and\n" +
         "# limitations under the License.\n" +
         "#\n";

      public void write(PrintWriter fileWriter) throws IOException {
         fileWriter.write(this.licenseText);
      }
   }

   private class LoggerNamesSection extends LoggingSection {
      LinkedHashSet<String> loggers = new LinkedHashSet<>();

      @Override
      public void write(PrintWriter fileWriter) throws IOException {
         StringBuilder builder = new StringBuilder();
         Iterator<String> loggerIter = loggers.iterator();
         while (loggerIter.hasNext()) {
            String logger = loggerIter.next();
            builder.append(logger);
            if (loggerIter.hasNext()) {
               builder.append(",");
            }
         }
         super.write(fileWriter);
         fileWriter.write("loggers=" + builder.toString());
      }

      // add or update
      public void addLogger(String loggerName) {
         loggers.add(loggerName);
      }

      // value being a comma separated string
      public void updateLoggers(String value) {
         String[] names = value.split(",");
         for (String l : names) {
            loggers.add(l);
         }
      }
   }

   private class LoggingSection {
      protected List<String> comment = new ArrayList<>();
      protected LinkedHashMap<String, ConfigEntry> loggingConfigs = new LinkedHashMap<>();

      public void addConfigEntry(String commentID, ConfigEntry entry) {
         loggingConfigs.put(commentID, entry);
      }

      public void addComment(String comment) {
         this.comment.add(comment);
      }

      public void write(PrintWriter fileWriter) throws IOException {
         fileWriter.println();
         for (String cmmt : comment) {
            fileWriter.println("# " + cmmt);
         }
         Boolean commentWritten = false;
         Iterator<Map.Entry<String, ConfigEntry>> iter = loggingConfigs.entrySet().iterator();
         while (iter.hasNext()) {
            Map.Entry<String, ConfigEntry> entry = iter.next();
            commentWritten = entry.getValue().write(fileWriter, commentWritten);
         }
      }

      public String addLogger(String loggerName, String logLevel) {
         LoggerPropertyEntry loggerEntry = (LoggerPropertyEntry) loggingConfigs.get(loggerName);
         if (loggerEntry != null) {
            loggerEntry.updateLogLevel(logLevel);
            return null;
         }
         StringBuilder builder = new StringBuilder("logger.");
         if (loggerName != null && !loggerName.isEmpty()) {
            builder.append(loggerName);
            builder.append(".");
         }
         builder.append("level");
         loggingConfigs.put(loggerName, new LoggerPropertyEntry(loggerName, builder.toString(), logLevel));
         return loggerName;
      }

      public String addLoggerHandler(String loggerName, String handlerNames) {
         LoggerPropertyEntry entry = (LoggerPropertyEntry) loggingConfigs.get(loggerName);
         if (entry != null) {
            entry.updateLogHandlers(handlerNames);
            return null;
         }
         StringBuilder builder = new StringBuilder("logger.");
         if (loggerName != null && !loggerName.isEmpty()) {
            builder.append(loggerName).append(".");
         }
         builder.append("handlers");
         LoggerPropertyEntry newEntry = new LoggerPropertyEntry(loggerName, builder.toString(), handlerNames);
         loggingConfigs.put(loggerName, newEntry);
         return loggerName;
      }

      public String addLoggerUseParentHandlersFlag(String loggerName, boolean flag) {
         LoggerPropertyEntry entry = (LoggerPropertyEntry) loggingConfigs.get(loggerName);
         if (entry != null) {
            entry.updateLoggerUseParentHandlersFlag(flag);
            return null;
         }
         StringBuilder builder = new StringBuilder("logger.");
         if (loggerName != null && !loggerName.isEmpty()) {
            builder.append(loggerName).append(".");
         }
         builder.append("useParentHandlers");
         loggingConfigs.put(loggerName, new LoggerPropertyEntry(loggerName, builder.toString(), flag ? "true" : "false"));
         return loggerName;
      }

      // possible key/values:
      // logger.<logger>.level : level
      // logger.<logger>.handlers : handlers
      // logger.<logger>.useParentHandlers : true/false
      public String updateLogger(String key, String value) {
         String loggerName = "";
         String newLogger = null;
         if (key.endsWith(".level")) {
            if (!key.equals("logger.level")) {
               loggerName = key.substring(7, key.length() - 6);
            }
            newLogger = this.addLogger(loggerName, value);
         } else if (key.endsWith(".handlers")) {
            if (!key.equals("logger.handlers")) {
               loggerName = key.substring(7, key.length() - 9);
            }
            newLogger = this.addLoggerHandler(loggerName, value);
         } else if (key.endsWith(".useParentHandlers")) {
            loggerName = key.substring(7, key.length() - 18);
            newLogger = this.addLoggerUseParentHandlersFlag(loggerName, value.equals("true") ? true : false);
         }
         return newLogger;
      }
   }

   private class FormatterPropertyEntry extends ConfigEntry {
      private String formatterName;
      private LinkedHashMap<String, String> formatterProps = new LinkedHashMap<>();

      FormatterPropertyEntry(String formatterName) {
         super(null);
         this.formatterName = formatterName;
      }

      FormatterPropertyEntry(String formatterName, String key, String value) {
         super(null);
         this.formatterName = formatterName;
         formatterProps.put(key, value);
      }

      public void updateFormatterClass(String formatterClass) {
         String handlersKey = "formatter." + formatterName;
         formatterProps.put(handlersKey, formatterClass);
      }

      private void updateFormatterPropertiesNames(String propertyNames) {
         String formatterKey = "formatter." + formatterName + ".properties";
         formatterProps.put(formatterKey, propertyNames);
      }

      private void updateFormatterPropertiesValues(String[] propertyNames, List<String> propertyValues) {
         for (int i = 0; i < propertyNames.length; i++) {
            String propKey = "formatter." + formatterName + "." + propertyNames[i];
            formatterProps.put(propKey, propertyValues.get(i));
         }
      }

      public void updateFormatterProperties(String propertyNames, List<String> propertyValues) {
         updateFormatterPropertiesNames(propertyNames);
         String[] propNames = propertyNames.split(",");
         updateFormatterPropertiesValues(propNames, propertyValues);
      }

      public void updateFormatterPropertyNames(String formatterName, String value) {
         String formattersKey = "formatter." + formatterName + ".properties";
         formatterProps.put(formattersKey, value);
      }

      public void updateFormatterProperty(String propertyName, String propertyValue) {
         String propKey = "formatter." + formatterName + "." + propertyName;
         formatterProps.put(propKey, propertyValue);
      }

      @Override
      protected Boolean write(PrintWriter fileWriter, Boolean commentWritten) throws IOException {
         Iterator<Map.Entry<String, String>> iter = formatterProps.entrySet().iterator();
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            fileWriter.write(ent.getKey() + "=" + ent.getValue() + "\n");
         }
         return false;
      }

   }

   private class HandlerPropertyEntry extends ConfigEntry {
      private String handlerName;
      private LinkedHashMap<String, String> handlerProps = new LinkedHashMap<>();

      HandlerPropertyEntry(String handlerName) {
         super(null);
         this.handlerName = handlerName;
      }

      HandlerPropertyEntry(String handlerName, String key, String value) {
         super(null);
         this.handlerName = handlerName;
         handlerProps.put(key, value);
      }

      @Override
      protected Boolean write(PrintWriter fileWriter, Boolean commentWritten) throws IOException {
         Iterator<Map.Entry<String, String>> iter = handlerProps.entrySet().iterator();
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            fileWriter.write(ent.getKey() + "=" + ent.getValue() + "\n");
         }
         return false;
      }

      public void updateHandlerClass(String handlerClass) {
         String handlersKey = "handler." + handlerName;
         handlerProps.put(handlersKey, handlerClass);
      }

      public void updateHandlerPropertyNames(String propertyNames) {
         String handlersKey = "handler." + handlerName + ".properties";
         handlerProps.put(handlersKey, propertyNames);
      }

      public void updateHandlerPropertyValues(String[] propertyNames, List<String> propertyValues) {
         for (int i = 0; i < propertyNames.length; i++) {
            String propKey = "handler." + handlerName + "." + propertyNames[i];
            handlerProps.put(propKey, propertyValues.get(i));
         }
      }

      public void updateHandlerProperty(String propertyName, String propertyValue) {
         String propKey = "handler." + handlerName + "." + propertyName;
         handlerProps.put(propKey, propertyValue);
      }

      public void updateHandlerProperties(String propertyNames, List<String> propertyValues) {
         updateHandlerPropertyNames(propertyNames);
         String[] propNames = propertyNames.split(",");
         updateHandlerPropertyValues(propNames, propertyValues);
      }

      public void updateHandlerLevel(String level) {
         String handlersKey = "handler." + handlerName + ".level";
         handlerProps.put(handlersKey, level);
      }

      public void updateHandlerFormatter(String formatter) {
         String handlersKey = "handler." + handlerName + ".formatter";
         handlerProps.put(handlersKey, formatter);
      }

   }

   private class LoggerPropertyEntry extends ConfigEntry {
      private String loggerName;
      private LinkedHashMap<String, String> loggerProps = new LinkedHashMap<>();

      LoggerPropertyEntry(String loggerName, String key, String value) {
         super(null);
         this.loggerName = loggerName;
         loggerProps.put(key, value);
      }

      @Override
      protected Boolean write(PrintWriter fileWriter, Boolean commentWritten) throws IOException {
         Iterator<Map.Entry<String, String>> iter = loggerProps.entrySet().iterator();
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            fileWriter.write(ent.getKey() + "=" + ent.getValue() + "\n");
         }
         return false;
      }

      private boolean isRootLogger() {
         return loggerName == null || loggerName.isEmpty();
      }

      public void updateLogLevel(String logLevel) {
         Iterator<Map.Entry<String, String>> iter = loggerProps.entrySet().iterator();
         String levelKey = null;
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            if (ent.getKey().endsWith(".level")) {
               levelKey = ent.getKey();
               break;
            }
         }
         if (levelKey == null) {
            if (isRootLogger()) {
               levelKey = "logger.level";
            } else {
               levelKey = "logger." + loggerName + ".level";
            }
         }
         loggerProps.put(levelKey, logLevel);
      }

      public void updateLogHandlers(String handlersName) {
         Iterator<Map.Entry<String, String>> iter = loggerProps.entrySet().iterator();
         String handlersKey = null;
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            if (ent.getKey().endsWith(".handlers")) {
               handlersKey = ent.getKey();
               break;
            }
         }
         if (handlersKey == null) {
            if (isRootLogger()) {
               handlersKey = "logger.handlers";
            } else {
               handlersKey = "logger." + loggerName + ".handlers";
            }
         }
         loggerProps.put(handlersKey, handlersName);
      }

      public void updateLoggerUseParentHandlersFlag(boolean flag) {
         Iterator<Map.Entry<String, String>> iter = loggerProps.entrySet().iterator();
         String useParentHandlersKey = null;
         while (iter.hasNext()) {
            Map.Entry<String, String> ent = iter.next();
            if (ent.getKey().endsWith(".useParentHandlers")) {
               useParentHandlersKey = ent.getKey();
               break;
            }
         }
         if (useParentHandlersKey == null) {
            if (isRootLogger()) {
               useParentHandlersKey = "logger.useParentHandlers";
            } else {
               useParentHandlersKey = "logger." + loggerName + ".useParentHandlers";
            }
         }
         loggerProps.put(useParentHandlersKey, flag ? "true" : "false");
      }
   }

   private static class ConfigEntry {

      private String comment;

      private ConfigEntry(String comment) {
         this.comment = comment;
      }

      protected Boolean write(PrintWriter fileWriter, Boolean commentWritten) throws IOException {
         fileWriter.println((commentWritten ? "" : "\n") + "# " + comment);
         return true;
      }

      @Override
      public String toString() {
         return comment;
      }
   }

   private class LoggerHandlerSection extends LoggingSection {

      public void addHandler(String handlerName, String handlerClass) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry != null) {
            handlerEntry.updateHandlerClass(handlerClass);
            return;
         }

         String handlerKey = "handler." + handlerName;
         loggingConfigs.put(handlerName, new HandlerPropertyEntry(handlerName, handlerKey, handlerClass));
      }

      public void addHandlerPropertyNames(String handlerName, String propertyNames) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry == null) {
            handlerEntry = new HandlerPropertyEntry(handlerName);
            loggingConfigs.put(handlerName, handlerEntry);
         }
         handlerEntry.updateHandlerPropertyNames(propertyNames);
      }

      public void addHandlerProperties(String handlerName, String propertyNames, List<String> propertyValues) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry == null) {
            handlerEntry = new HandlerPropertyEntry(handlerName);
            loggingConfigs.put(handlerName, handlerEntry);
         }
         handlerEntry.updateHandlerProperties(propertyNames, propertyValues);
      }

      public void addHandlerLevel(String handlerName, String level) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry == null) {
            handlerEntry = new HandlerPropertyEntry(handlerName);
            loggingConfigs.put(handlerName, handlerEntry);
         }
         handlerEntry.updateHandlerLevel(level);
      }

      public void addHandlerFormatter(String handlerName, String formatter) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry == null) {
            handlerEntry = new HandlerPropertyEntry(handlerName);
            loggingConfigs.put(handlerName, handlerEntry);
         }
         handlerEntry.updateHandlerFormatter(formatter);
      }

      public void addHandler(String handlerName, String handlerClass, String propertyNames, List<String> propertyValues, String level,
                             String formatter) {
         addHandler(handlerName, handlerClass);
         addHandlerProperties(handlerName, propertyNames, propertyValues);
         addHandlerLevel(handlerName, level);
         addHandlerFormatter(handlerName, formatter);
      }

      // possible key/values:
      // handler.<handler> : class name
      // handler.<handler>.level : handler level
      // handler.<handler>.properties : comma separated property names
      // handler.<handler>.<prop> : value
      // handler.<handler>.formatter : formatter name
      public void updateHandler(String key, String value) {
         String handlerName;
         if (key.endsWith(".level")) {
            handlerName = key.substring(8, key.length() - 6);
            this.addHandlerLevel(handlerName, value);
         } else if (key.endsWith(".properties")) {
            handlerName = key.substring(8, key.length() - 11);
            this.addHandlerPropertyNames(handlerName, value);
         } else if (key.endsWith(".formatter")) {
            handlerName = key.substring(8, key.length() - 10);
            this.addHandlerFormatter(handlerName, value);
         } else {
            int lastDotIndex = key.lastIndexOf(".");
            if (lastDotIndex == key.indexOf(".")) {
               //handler class
               handlerName = key.substring(8);
               this.addHandler(handlerName, value);
            } else {
               //handler property
               handlerName = key.substring(8, lastDotIndex);
               String propertyName = key.substring(lastDotIndex + 1);
               this.updateHandlerProperty(handlerName, propertyName, value);
            }
         }
      }

      private void updateHandlerProperty(String handlerName, String propertyName, String value) {
         HandlerPropertyEntry handlerEntry = (HandlerPropertyEntry) loggingConfigs.get(handlerName);
         if (handlerEntry == null) {
            handlerEntry = new HandlerPropertyEntry(handlerName);
            loggingConfigs.put(handlerName, handlerEntry);
         }
         handlerEntry.updateHandlerProperty(propertyName, value);
      }
   }

   private class LoggerFormatterSection extends LoggingSection {

      public void addFormatter(String formatterName, String formatterClass) {
         FormatterPropertyEntry formatterEntry = (FormatterPropertyEntry) loggingConfigs.get(formatterName);
         if (formatterEntry != null) {
            formatterEntry.updateFormatterClass(formatterClass);
            return;
         }

         String formatterKey = "formatter." + formatterName;
         loggingConfigs.put(formatterName, new FormatterPropertyEntry(formatterName, formatterKey, formatterClass));
      }

      public void addFormatterProperties(String formatterName, String propertyNames, List<String> propertyValues) {
         FormatterPropertyEntry formatterEntry = (FormatterPropertyEntry) loggingConfigs.get(formatterName);
         if (formatterEntry == null) {
            formatterEntry = new FormatterPropertyEntry(formatterName);
            loggingConfigs.put(formatterName, formatterEntry);
         }
         formatterEntry.updateFormatterProperties(propertyNames, propertyValues);
      }

      public void addFormatter(String formatterName, String formatterClass, String propertyNames, List<String> propValues) {
         addFormatter(formatterName, formatterClass);
         addFormatterProperties(formatterName, propertyNames, propValues);
      }

      // possible key/values:
      // formatter.<formatter> : class name
      // formatter.<formatter>.properties : comma separated property names
      // formatter.<formatter>.<prop> : value
      public void updateFormatter(String key, String value) {
         String formatterName;
         if (key.endsWith(".properties")) {
            formatterName = key.substring(10, key.length() - 11);
            this.addFormatterPropertyNames(formatterName, value);
         } else {
            int lastDotIndex = key.lastIndexOf(".");
            if (lastDotIndex == key.indexOf(".")) {
               //formatter class
               formatterName = key.substring(10);
               this.addFormatter(formatterName, value);
            } else {
               //handler property
               formatterName = key.substring(10, lastDotIndex);
               String propertyName = key.substring(lastDotIndex + 1);
               this.updateFormatterProperty(formatterName, propertyName, value);
            }
         }
      }

      private void updateFormatterProperty(String formatterName, String propertyName, String value) {
         FormatterPropertyEntry formatterEntry = (FormatterPropertyEntry) loggingConfigs.get(formatterName);
         if (formatterEntry == null) {
            formatterEntry = new FormatterPropertyEntry(formatterName);
            loggingConfigs.put(formatterName, formatterEntry);
         }
         formatterEntry.updateFormatterProperty(propertyName, value);
      }

      private void addFormatterPropertyNames(String formatterName, String value) {
         FormatterPropertyEntry formatterEntry = (FormatterPropertyEntry) loggingConfigs.get(formatterName);
         if (formatterEntry == null) {
            formatterEntry = new FormatterPropertyEntry(formatterName);
            loggingConfigs.put(formatterName, formatterEntry);
         }
         formatterEntry.updateFormatterPropertyNames(formatterName, value);
      }
   }
}
