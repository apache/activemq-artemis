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

package org.apache.activemq.artemis.core.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

import org.jboss.logmanager.config.ErrorManagerConfiguration;
import org.jboss.logmanager.config.FilterConfiguration;
import org.jboss.logmanager.config.FormatterConfiguration;
import org.jboss.logmanager.config.HandlerConfiguration;
import org.jboss.logmanager.config.HandlerContainingConfigurable;
import org.jboss.logmanager.config.LogContextConfiguration;
import org.jboss.logmanager.config.LoggerConfiguration;
import org.jboss.logmanager.config.PojoConfiguration;
import org.jboss.logmanager.config.PropertyConfigurable;
import org.jboss.logmanager.config.ValueExpression;

public class LoggingConfigurationUpdater {

   private static final String[] EMPTY_STRINGS = new String[0];
   private static final Pattern EXPRESSION_PATTERN = Pattern.compile(".*\\$\\{.*\\}.*");
   private static final String LOGGERS = "loggers";
   private static final String HANDLERS = "handlers";
   private static final String FILTERS = "filters";
   private static final String FORMATTERS = "formatters";
   private static final String ERROR_MANAGERS = "errorManagers";
   private static final String POJOS = "pojos";
   private static final String LOGGER = "logger";
   private static final String LEVEL = "level";
   private static final String HANDLER = "handler";
   private static final String FORMATTER = "formatter";
   private static final String ENCODING = "encoding";
   private static final String ERROR_MANAGER = "errorManager";
   private static final String POST_CONFIGURATION = "postConfiguration";
   private static final String POJO = "pojo";
   private static final String MODULE = "module";
   private static final String PROPERTIES = "properties";
   private static final String FILTER = "filter";
   private static final String CONSTRUCTOR_PROPERTIES = "constructorProperties";
   private static final String USE_PARENT_HANDLERS = "useParentHandlers";

   private final LogContextConfiguration config;

   public LoggingConfigurationUpdater(final LogContextConfiguration config) {
      this.config = config;
   }

   /**
    * {@inheritDoc}
    */
   public void configure(final InputStream inputStream) throws IOException {
      final Properties properties = new Properties();
      try {
         properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
         inputStream.close();
      } finally {
         safeClose(inputStream);
      }
      configure(properties);
   }

   /**
    * Configure the log manager from the given properties.
    * <p/>
    * The following values read in from a configuration will be trimmed of prefixed and trailing whitespace:
    * <pre>
    *     <ul>
    *         <li>logger.NAME.filter</li>
    *         <li>logger.NAME.level</li>
    *         <li>logger.NAME.useParentHandlers</li>
    *         <li>handler.NAME.filter</li>
    *         <li>handler.NAME.formatter</li>
    *         <li>handler.NAME.level</li>
    *         <li>handler.NAME.encoding</li>
    *         <li>handler.NAME.errorManager</li>
    *     </ul>
    * </pre>
    *
    * @param properties the properties
    */
   private void configure(final Properties properties) {
      try {
         final Collection<String> handlersToRemove = config.getHandlerNames();
         // Start with the list of loggers to configure.  The root logger is always on the list.
         handlersToRemove.removeAll(configureLogger(properties, ""));

         // And, for each logger name, configure any filters, handlers, etc.
         final String[] loggerNames = getStringCsvArray(properties, LOGGERS);
         for (String loggerName : loggerNames) {
            handlersToRemove.removeAll(configureLogger(properties, loggerName));
         }
         // Remove any loggers that are not longer required
         final Collection<String> loggersToRemove = config.getLoggerNames();
         loggersToRemove.remove("");
         loggersToRemove.removeAll(Arrays.asList(loggerNames));
         for (String loggerName : loggersToRemove) {
            config.removeLoggerConfiguration(loggerName);
         }

         // Configure any declared handlers.
         final String[] handlerNames = getStringCsvArray(properties, HANDLERS);
         for (String handlerName : handlerNames) {
            configureHandler(properties, handlerName);
         }
         // Remove any handlers that are not longer required
         handlersToRemove.removeAll(Arrays.asList(handlerNames));
         for (String handlerName : handlersToRemove) {
            config.removeHandlerConfiguration(handlerName);
         }

         // Configure any declared filters.
         for (String filterName : getStringCsvArray(properties, FILTERS)) {
            configureFilter(properties, filterName);
         }

         // Configure any declared formatters.
         for (String formatterName : getStringCsvArray(properties, FORMATTERS)) {
            configureFormatter(properties, formatterName);
         }

         // Configure any declared error managers.
         for (String errorManagerName : getStringCsvArray(properties, ERROR_MANAGERS)) {
            configureErrorManager(properties, errorManagerName);
         }

         // Configure POJOs
         for (String pojoName : getStringCsvArray(properties, POJOS)) {
            configurePojos(properties, pojoName);
         }
         config.commit();
      } finally {
         config.forget();
      }
   }

   private List<String> configureLogger(final Properties properties, final String loggerName) {
      final LoggerConfiguration loggerConfiguration;
      if (config.getLoggerNames().contains(loggerName)) {
         loggerConfiguration = config.getLoggerConfiguration(loggerName);
      } else {
         loggerConfiguration = config.addLoggerConfiguration(loggerName);
      }

      // Get logger level
      final String levelName = getStringProperty(properties, getKey(LOGGER, loggerName, LEVEL));
      if (notEqual(levelName, loggerConfiguration.getLevelValueExpression())) {
         loggerConfiguration.setLevel(levelName == null ? "ALL" : levelName);
      }

      // Get logger filter
      final String filterName = getStringProperty(properties, getKey(LOGGER, loggerName, FILTER));
      final ValueExpression<String> newValue = ValueExpression.STRING_RESOLVER.resolve(filterName);
      if (notEqual(newValue, loggerConfiguration.getFilterValueExpression())) {
         loggerConfiguration.setFilter(filterName);
         final String resolvedFilter = loggerConfiguration.getFilterValueExpression().getResolvedValue();
         if (resolvedFilter != null) {
            // Check for a filter class
            final String filterClassName = getStringProperty(properties, getKey(FILTER, resolvedFilter));
            // If the filter class is null, assume it's a filter expression
            if (filterClassName != null) {
               configureFilter(properties, resolvedFilter);
            }
         }
      }

      // Get logger handlers
      configureHandlerNames(properties, loggerConfiguration, LOGGER, loggerName);

      // Get logger properties
      final String useParentHandlersString = getStringProperty(properties, getKey(LOGGER, loggerName, USE_PARENT_HANDLERS));
      if (booleanNotEqual(useParentHandlersString, loggerConfiguration.getUseParentHandlersValueExpression())) {
         // Check for expression
         if (EXPRESSION_PATTERN.matcher(useParentHandlersString).matches()) {
            loggerConfiguration.setUseParentHandlers(useParentHandlersString);
         } else {
            loggerConfiguration.setUseParentHandlers(Boolean.parseBoolean(useParentHandlersString));
         }
      }
      return loggerConfiguration.getHandlerNames();
   }

   private void configureFilter(final Properties properties, final String filterName) {
      final String className = getStringProperty(properties, getKey(FILTER, filterName));
      if (className == null) {
         // Assume we're using a filter expression
         return;
      }
      final FilterConfiguration configuration;
      if (config.getFilterNames().contains(filterName)) {
         configuration = config.getFilterConfiguration(filterName);
      } else {
         configuration = config.addFilterConfiguration(getStringProperty(properties, getKey(FILTER, filterName, MODULE)), className, filterName, getStringCsvArray(properties, getKey(FILTER, filterName, CONSTRUCTOR_PROPERTIES)));
      }
      final String[] postConfigurationMethods = getStringCsvArray(properties, getKey(FILTER, filterName, POST_CONFIGURATION));
      configuration.setPostConfigurationMethods(postConfigurationMethods);
      configureProperties(properties, configuration, getKey(FILTER, filterName));
   }

   private boolean configureFormatter(final Properties properties, final String formatterName) {
      final String className = getStringProperty(properties, getKey(FORMATTER, formatterName));
      if (className == null) {
         printError("Formatter %s is not defined%n", formatterName);
         return false;
      }
      final FormatterConfiguration configuration;
      if (config.getFormatterNames().contains(formatterName)) {
         configuration = config.getFormatterConfiguration(formatterName);
      } else {
         configuration = config.addFormatterConfiguration(getStringProperty(properties, getKey(FORMATTER, formatterName, MODULE)), className, formatterName, getStringCsvArray(properties, getKey(FORMATTER, formatterName, CONSTRUCTOR_PROPERTIES)));
      }
      final String[] postConfigurationMethods = getStringCsvArray(properties, getKey(FORMATTER, formatterName, POST_CONFIGURATION));
      configuration.setPostConfigurationMethods(postConfigurationMethods);
      configureProperties(properties, configuration, getKey(FORMATTER, formatterName));
      return true;
   }

   private boolean configureErrorManager(final Properties properties, final String errorManagerName) {
      final String className = getStringProperty(properties, getKey(ERROR_MANAGER, errorManagerName));
      if (className == null) {
         printError("Error manager %s is not defined%n", errorManagerName);
         return false;
      }
      final ErrorManagerConfiguration configuration;
      if (config.getErrorManagerNames().contains(errorManagerName)) {
         configuration = config.getErrorManagerConfiguration(errorManagerName);
      } else {
         configuration = config.addErrorManagerConfiguration(getStringProperty(properties, getKey(ERROR_MANAGER, errorManagerName, MODULE)), className, errorManagerName, getStringCsvArray(properties, getKey(ERROR_MANAGER, errorManagerName, CONSTRUCTOR_PROPERTIES)));
      }
      final String[] postConfigurationMethods = getStringCsvArray(properties, getKey(ERROR_MANAGER, errorManagerName, POST_CONFIGURATION));
      configuration.setPostConfigurationMethods(postConfigurationMethods);
      configureProperties(properties, configuration, getKey(ERROR_MANAGER, errorManagerName));
      return true;
   }

   private boolean configureHandler(final Properties properties, final String handlerName) {
      final String className = getStringProperty(properties, getKey(HANDLER, handlerName));
      if (className == null) {
         printError("Handler %s is not defined%n", handlerName);
         return false;
      }
      final HandlerConfiguration configuration;
      if (config.getHandlerNames().contains(handlerName)) {
         configuration = config.getHandlerConfiguration(handlerName);
      } else {
         configuration = config.addHandlerConfiguration(getStringProperty(properties, getKey(HANDLER, handlerName, MODULE)), className, handlerName, getStringCsvArray(properties, getKey(HANDLER, handlerName, CONSTRUCTOR_PROPERTIES)));
      }
      final String filter = getStringProperty(properties, getKey(HANDLER, handlerName, FILTER));
      if (notEqual(filter, configuration.getFilterValueExpression())) {
         configuration.setFilter(filter);
         final String resolvedFilter = configuration.getFilterValueExpression().getResolvedValue();
         if (resolvedFilter != null) {
            // Check for a filter class
            final String filterClassName = getStringProperty(properties, getKey(FILTER, resolvedFilter));
            // If the filter class is null, assume it's a filter expression
            if (filterClassName != null) {
               configureFilter(properties, resolvedFilter);
            }
         }
      }
      final String levelName = getStringProperty(properties, getKey(HANDLER, handlerName, LEVEL));
      if (notEqual(levelName, configuration.getLevelValueExpression())) {
         configuration.setLevel(levelName == null ? "ALL" : levelName);
      }
      final String formatterName = getStringProperty(properties, getKey(HANDLER, handlerName, FORMATTER));
      if (formatterName != null) {
         if (getStringProperty(properties, getKey(FORMATTER, ValueExpression.STRING_RESOLVER.resolve(formatterName).getResolvedValue())) == null) {
            printError("Formatter %s is not defined%n", formatterName);
         } else {
            final ValueExpression<String> newValue = ValueExpression.STRING_RESOLVER.resolve(formatterName);
            if (notEqual(newValue, configuration.getFormatterNameValueExpression())) {
               if (configureFormatter(properties, newValue.getResolvedValue())) {
                  configuration.setFormatterName(formatterName);
               }
            }
         }
      }
      final String encoding = getStringProperty(properties, getKey(HANDLER, handlerName, ENCODING));
      if (notEqual(encoding, configuration.getEncodingValueExpression())) {
         configuration.setEncoding(encoding);
      }
      final String errorManagerName = getStringProperty(properties, getKey(HANDLER, handlerName, ERROR_MANAGER));
      if (errorManagerName != null) {
         if (getStringProperty(properties, getKey(ERROR_MANAGER, ValueExpression.STRING_RESOLVER.resolve(errorManagerName).getResolvedValue())) == null) {
            printError("Error manager %s is not defined%n", errorManagerName);
         } else {
            final ValueExpression<String> newValue = ValueExpression.STRING_RESOLVER.resolve(errorManagerName);
            if (notEqual(newValue, configuration.getErrorManagerNameValueExpression())) {
               if (configureErrorManager(properties, newValue.getResolvedValue())) {
                  configuration.setErrorManagerName(errorManagerName);
               }
            }
         }
      }
      configureHandlerNames(properties, configuration, HANDLER, handlerName);
      final String[] postConfigurationMethods = getStringCsvArray(properties, getKey(HANDLER, handlerName, POST_CONFIGURATION));
      configuration.setPostConfigurationMethods(postConfigurationMethods);
      configureProperties(properties, configuration, getKey(HANDLER, handlerName));
      return true;
   }

   private void configurePojos(final Properties properties, final String pojoName) {
      final String className = getStringProperty(properties, getKey(POJO, pojoName));
      if (className == null) {
         printError("POJO %s is not defined%n", pojoName);
         return;
      }
      final PojoConfiguration configuration;
      if (config.getPojoNames().contains(pojoName)) {
         configuration = config.getPojoConfiguration(pojoName);
      } else {
         configuration = config.addPojoConfiguration(getStringProperty(properties, getKey(POJO, pojoName, MODULE)), getStringProperty(properties, getKey(POJO, pojoName)), pojoName, getStringCsvArray(properties, getKey(POJO, pojoName, CONSTRUCTOR_PROPERTIES)));
      }
      final String[] postConfigurationMethods = getStringCsvArray(properties, getKey(POJO, pojoName, POST_CONFIGURATION));
      configuration.setPostConfigurationMethods(postConfigurationMethods);
      configureProperties(properties, configuration, getKey(POJO, pojoName));
   }

   private void configureProperties(final Properties properties,
                                    final PropertyConfigurable configurable,
                                    final String prefix) {
      final List<String> propertyNames = getStringCsvList(properties, getKey(prefix, PROPERTIES));
      for (String propertyName : propertyNames) {
         final String valueString = getStringProperty(properties, getKey(prefix, propertyName), false);
         if (notEqual(valueString, configurable.getPropertyValueExpression(propertyName))) {
            configurable.setPropertyValueString(propertyName, valueString);
         }
      }
   }

   private void configureHandlerNames(final Properties properties,
                                      final HandlerContainingConfigurable configuration,
                                      final String prefix,
                                      final String name) {
      final String[] handlerNames = getStringCsvArray(properties, getKey(prefix, name, HANDLERS));
      final Collection<String> availableHandlers = new ArrayList<>();
      for (String handlerName : handlerNames) {
         if (configureHandler(properties, handlerName)) {
            availableHandlers.add(handlerName);
         }
      }
      configuration.setHandlerNames(availableHandlers);
   }

   private static String getKey(final String prefix, final String objectName) {
      return objectName.length() > 0 ? prefix + "." + objectName : prefix;
   }

   private static String getKey(final String prefix, final String objectName, final String key) {
      return objectName.length() > 0 ? prefix + "." + objectName + "." + key : prefix + "." + key;
   }

   private static String getStringProperty(final Properties properties, final String key) {
      return getStringProperty(properties, key, true);
   }

   private static String getStringProperty(final Properties properties, final String key, final boolean trim) {
      final String value = properties.getProperty(key);
      return (trim ? (value == null ? null : value.trim()) : value);
   }

   private static String[] getStringCsvArray(final Properties properties, final String key) {
      final String property = properties.getProperty(key, "");
      if (property == null) {
         return EMPTY_STRINGS;
      }
      final String value = property.trim();
      if (value.length() == 0) {
         return EMPTY_STRINGS;
      }
      return value.split("\\s*,\\s*");
   }

   private static List<String> getStringCsvList(final Properties properties, final String key) {
      return new ArrayList<>(Arrays.asList(getStringCsvArray(properties, key)));
   }

   private static void printError(final String format, final Object... args) {
      System.err.printf(format, args);
   }

   private static void safeClose(final Closeable stream) {
      if (stream != null)
         try {
            stream.close();
         } catch (Exception e) {
            // can't do anything about it
         }
   }

   private static boolean notEqual(final ValueExpression<String> newValue, final ValueExpression<String> currentValue) {
      if (newValue == null) {
         return currentValue.getResolvedValue() != null;
      }
      return !Objects.equals(newValue.getValue(), currentValue.getValue());
   }

   private static boolean notEqual(final String newValue, final ValueExpression<String> currentValue) {
      if (newValue == null) {
         return currentValue.getResolvedValue() != null;
      }
      if (currentValue.isExpression()) {
         final String resolvedCurrentValue = currentValue.getResolvedValue();
         final String resolvedNewValue = ValueExpression.STRING_RESOLVER.resolve(newValue).getResolvedValue();
         return resolvedCurrentValue == null ? resolvedNewValue != null : !resolvedCurrentValue.equals(resolvedNewValue);
      }
      return !newValue.equals(currentValue.getValue());
   }

   private static boolean booleanNotEqual(final String newValue, final ValueExpression<Boolean> currentValue) {
      if (newValue == null) {
         return currentValue.getResolvedValue() != null;
      }
      if (currentValue.isExpression()) {
         final Boolean resolvedCurrentValue = currentValue.getResolvedValue();
         final Boolean resolvedNewValue = ValueExpression.BOOLEAN_RESOLVER.resolve(newValue).getResolvedValue();
         return resolvedCurrentValue == null ? resolvedNewValue != null : !resolvedCurrentValue.equals(resolvedNewValue);
      }
      return !newValue.equals(currentValue.getValue());
   }

}