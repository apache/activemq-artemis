/**
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

package org.apache.activemq.artemis.common.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ExampleUtil
{
   public static Process startServer(String artemisInstance, String serverName) throws Exception
   {
      boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");

      ProcessBuilder builder = null;
      if (IS_WINDOWS)
      {
         builder = new ProcessBuilder("cmd", "/c", "artemis.cmd", "run");
      }
      else
      {
         builder = new ProcessBuilder("./artemis", "run");
      }

      builder.directory(new File(artemisInstance + "/bin"));

      Process process = builder.start();

      ProcessLogger outputLogger = new ProcessLogger(true,
                                                     process.getInputStream(),
                                                     serverName + "::Out",
                                                     false);
      outputLogger.start();

      // Adding a reader to System.err, so the VM won't hang on a System.err.println as identified on this forum thread:
      // http://www.jboss.org/index.html?module=bb&op=viewtopic&t=151815
      ProcessLogger errorLogger = new ProcessLogger(true,
                                                    process.getErrorStream(),
                                                    serverName + "::Err",
                                                    true);
      errorLogger.start();

      return process;
   }


   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread
   {
      private final InputStream is;

      private final String logName;

      private final boolean print;

      private final boolean sendToErr;

      boolean failed = false;

      ProcessLogger(final boolean print,
                    final InputStream is,
                    final String logName,
                    final boolean sendToErr) throws ClassNotFoundException
      {
         this.is = is;
         this.print = print;
         this.logName = logName;
         this.sendToErr = sendToErr;
         setDaemon(false);
      }

      @Override
      public void run()
      {
         try
         {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null)
            {
               if (print)
               {
                  if (sendToErr)
                  {
                     System.err.println(logName + " err:" + line);
                  }
                  else
                  {
                     System.out.println(logName + " out:" + line);
                  }
               }
            }
         }
         catch (IOException e)
         {
            // ok, stream closed
         }

      }
   }

}
