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
package org.apache.activemq.artemis.logs;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

/**
 * This class contains a tool where programs could intercept for LogMessage given an interval of time between {@link #startCapture()}
 * and {@link #stopCapture()}
 *
 * Be careful with this use as this is intended for testing only (such as testcases)
 */

@Plugin(name = "AssertionLoggerHandler", category = "Core", elementType = "appender")
public class AssertionLoggerHandler extends AbstractAppender {

   private static final Map<String, LogEvent> messages = new ConcurrentHashMap<>();
   private static List<String> traceMessages;
   private static volatile boolean capture = false;
   private static volatile boolean captureStackTrace = false;


   public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B> implements org.apache.logging.log4j.core.util.Builder<AssertionLoggerHandler> {
      @Override
      public AssertionLoggerHandler build() {
         return new AssertionLoggerHandler(getName(), getFilter(), getOrCreateLayout(), isIgnoreExceptions(), getPropertyArray());
      }
   }

   @PluginBuilderFactory
   public static <B extends Builder<B>> B newBuilder() {
      return new Builder<B>().asBuilder();
   }

   protected AssertionLoggerHandler(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, Property[] properties) {
      super(name, filter, layout, ignoreExceptions, properties);
   }

   @Override
   public void append(LogEvent event) {
      if (capture) {
         String formattedMessage = event.getMessage().getFormattedMessage();

         if (captureStackTrace && event.getThrown() != null) {
            StringWriter stackOutput = new StringWriter();
            event.getThrown().printStackTrace(new PrintWriter(stackOutput));
            formattedMessage += stackOutput.toString();
         }

         messages.put(formattedMessage, event);

         if (traceMessages != null) {
            traceMessages.add(formattedMessage);
         }
      }
   }

   /**
    * is there any record matching Level?
    *
    * @param level
    * @return
    */
   public static boolean hasLevel(LogLevel level) {
      Level implLevel = level.toImplLevel();

      for (LogEvent event : messages.values()) {
         if (implLevel.equals(event.getLevel())) {
            return true;
         }
      }

      return false;
   }

   public static LogLevel setLevel(String loggerName, LogLevel level) {
      final Logger logger = LogManager.getLogger(loggerName);
      final Level existingLevel = logger.getLevel();

      final Level newLevel = level.toImplLevel();

      if (!existingLevel.equals(newLevel)) {
         Configurator.setLevel(logger, newLevel);
      }

      return LogLevel.fromImplLevel(existingLevel);
   }

   public static boolean findText(long mstimeout, String... text) {

      long timeMax = System.currentTimeMillis() + mstimeout;
      do {
         if (findText(text)) {
            return true;
         }
      }
      while (timeMax > System.currentTimeMillis());

      return false;

   }

   /**
    * Find a line that contains the parameters passed as an argument
    *
    * @param text
    * @return
    */
   public static boolean findText(final String... text) {
      for (Map.Entry<String, LogEvent> entry : messages.entrySet()) {
         String key = entry.getKey();
         boolean found = true;

         for (String txtCheck : text) {
            found = key.contains(txtCheck);
            if (!found) {
               // If the main log message doesn't contain what we're looking for let's look in the message from the exception (if there is one).
               Throwable throwable = entry.getValue().getThrown();
               if (throwable != null && throwable.getMessage() != null) {
                  found = throwable.getMessage().contains(txtCheck);
                  if (!found) {
                     break;
                  }
               } else {
                  break;
               }
            }
         }

         if (found) {
            return true;
         }
      }

      return false;
   }

   public static int countText(final String... text) {
      int found = 0;
      if (traceMessages != null) {
         for (String str : traceMessages) {
            for (String txtCheck : text) {
               if (str.contains(txtCheck)) {
                  found++;
               }
            }
         }
      } else {
         for (Map.Entry<String, LogEvent> entry : messages.entrySet()) {
            String key = entry.getKey();

            for (String txtCheck : text) {
               if (key.contains(txtCheck)) {
                  found++;
               }
            }
         }
      }

      return found;
   }

   public static boolean matchText(final String pattern) {
      Pattern r = Pattern.compile(pattern);

      for (Map.Entry<String, LogEvent> entry : messages.entrySet()) {
         if (r.matcher(entry.getKey()).matches()) {
            return true;
         } else {
            Throwable throwable = entry.getValue().getThrown();
            if (throwable != null && throwable.getMessage() != null) {
               if (r.matcher(throwable.getMessage()).matches()) {
                  return true;
               }
            }
         }
      }

      return false;
   }

   public static final void clear() {
      messages.clear();
      if (traceMessages != null) {
         traceMessages.clear();
      }
   }

   public static final void startCapture() {
      startCapture(false);
   }

   /**
    *
    * @param individualMessages enables counting individual messages.
    */
   public static final void startCapture(boolean individualMessages) {
      startCapture(individualMessages, captureStackTrace);
   }

   /**
    *
    * @param individualMessages enables counting individual messages.
    */
   public static final void startCapture(boolean individualMessages, boolean captureStackTrace) {
      clear();
      if (individualMessages) {
         traceMessages = new LinkedList<>();
      }
      capture = true;
      AssertionLoggerHandler.captureStackTrace = captureStackTrace;
   }

   public static final void stopCapture() {
      capture = false;
      captureStackTrace = false;
      clear();
      traceMessages = null;
   }

   public enum LogLevel {
      OFF(Level.OFF),
      FATAL(Level.FATAL),
      ERROR(Level.ERROR),
      WARN(Level.WARN),
      INFO(Level.INFO),
      DEBUG(Level.DEBUG),
      TRACE(Level.TRACE);

      Level implLevel;

      LogLevel(Level implLevel) {
         this.implLevel = implLevel;
      }

      private Level toImplLevel() {
         return implLevel;
      }

      private static LogLevel fromImplLevel(Level implLevel) {
         if (Level.FATAL.equals(implLevel)) {
            return FATAL;
         } else if (Level.ERROR.equals(implLevel)) {
            return ERROR;
         } else if (Level.WARN.equals(implLevel)) {
            return WARN;
         } else if (Level.INFO.equals(implLevel)) {
            return INFO;
         } else if (Level.DEBUG.equals(implLevel)) {
            return DEBUG;
         } else if (Level.TRACE.equals(implLevel)) {
            return TRACE;
         } else if (Level.OFF.equals(implLevel)) {
            return OFF;
         } else {
            throw new IllegalArgumentException("Unexpected level:" + implLevel);
         }
      }
   }

}
