/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.logging;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import org.jboss.logmanager.ExtHandler;
import org.jboss.logmanager.ExtLogRecord;
import org.junit.Assert;

/**
 * @author <a href="mailto:ehugonne@redhat.com">Emmanuel Hugonnet</a> (c) 2013 Red Hat, inc.
 */
public class AssertionLoggerHandler extends ExtHandler
{

   private static final Map<String, ExtLogRecord> messages = new ConcurrentHashMap<>();
   private static boolean capture = false;

   /**
    * {@inheritDoc}
    */
   @Override
   public void flush()
   {
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void close() throws SecurityException
   {
   }

   @Override
   protected void doPublish(final ExtLogRecord record)
   {
      if (capture)
      {
         messages.put(record.getFormattedMessage(), record);
      }
   }

   public static void assertMessageWasLogged(String assertionMessage, String expectedMessage)
   {
      if (!messages.containsKey(expectedMessage))
      {
         throw new AssertionError(assertionMessage);
      }
   }

   public static void assertMessageWasLogged(String message)
   {
      if (!messages.containsKey(message))
      {
         throw new AssertionError(Arrays.toString(messages.keySet().toArray()));
      }
   }

   /**
    * Find a line that contains the parameters passed as an argument
    *
    * @param text
    * @return
    */
   public static boolean findText(final String... text)
   {
      for (Map.Entry<String, ExtLogRecord> entry : messages.entrySet())
      {
         String key = entry.getKey();
         boolean found = true;

         for (String txtCheck : text)
         {
            found = key.contains(txtCheck);
            if (!found)
            {
               // If the main log message doesn't contain what we're looking for let's look in the message from the exception (if there is one).
               Throwable throwable = entry.getValue().getThrown();
               if (throwable != null && throwable.getMessage() != null)
               {
                  found = throwable.getMessage().contains(txtCheck);
                  if (!found)
                  {
                     break;
                  }
               }
               else
               {
                  break;
               }
            }
         }

         if (found)
         {
            return true;
         }
      }

      return false;
   }

   public static void assertMessageWasLoggedWithLevel(String expectedMessage, Level expectedLevel)
   {
      if (!messages.containsKey(expectedMessage))
      {
         throw new AssertionError((Arrays.toString(messages.keySet().toArray())));
      }
      Assert.assertEquals(expectedLevel, messages.get(expectedMessage).getLevel());
   }

   public static void assertMessageWasLoggedWithLevel(String assertionMessage, String expectedMessage, Level expectedLevel)
   {
      if (!messages.containsKey(expectedMessage))
      {
         throw new AssertionError(assertionMessage);
      }
      Assert.assertEquals(assertionMessage, expectedLevel, messages.get(expectedMessage).getLevel());
   }

   public static final void clear()
   {
      messages.clear();
   }

   public static final void startCapture()
   {
      clear();
      capture = true;
   }

   public static final void stopCapture()
   {
      capture = false;
      clear();
   }
}
