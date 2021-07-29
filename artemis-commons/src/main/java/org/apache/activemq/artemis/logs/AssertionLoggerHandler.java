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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.regex.Pattern;

import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;

/**
 * This class contains a tool where programs could intercept for LogMessage given an interval of time between {@link #startCapture()}
 * and {@link #stopCapture()}
 *
 * Be careful with this use as this is intended for testing only (such as testcases)
 */
public class AssertionLoggerHandler extends ExtHandler {

   private static final Map<String, ExtLogRecord> messages = new ConcurrentHashMap<>();
   private static List<String> traceMessages;
   private static boolean capture = false;

   /**
    * {@inheritDoc}
    */
   @Override
   public void flush() {
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() throws SecurityException {
   }

   @Override
   protected void doPublish(final ExtLogRecord record) {
      if (capture) {
         messages.put(record.getFormattedMessage(), record);
         if (traceMessages != null) {
            traceMessages.add(record.getFormattedMessage());
         }
      }
   }

   /**
    * is there any record matching Level?
    *
    * @param level
    * @return
    */
   public static boolean hasLevel(Level level) {
      for (ExtLogRecord record : messages.values()) {
         if (record.getLevel().equals(level)) {
            return true;
         }
      }

      return false;
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
      for (Map.Entry<String, ExtLogRecord> entry : messages.entrySet()) {
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
         for (Map.Entry<String, ExtLogRecord> entry : messages.entrySet()) {
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

      for (Map.Entry<String, ExtLogRecord> entry : messages.entrySet()) {
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
      clear();
      if (individualMessages) {
         traceMessages = new LinkedList<>();
      }
      capture = true;
   }

   public static final void stopCapture() {
      capture = false;
      clear();
      traceMessages = null;
   }
}
