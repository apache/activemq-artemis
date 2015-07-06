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

package org.apache.activemq.artemis.util;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.Connection;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class ServerUtil
{
   public static Process startServer(String artemisInstance, String serverName) throws Exception
   {
      return startServer(artemisInstance, serverName, 0, 0);
   }

   public static Process startServer(String artemisInstance, String serverName, int id, int timeout) throws Exception
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

      final Process process = builder.start();
      Runtime.getRuntime().addShutdownHook(new Thread()
      {
         public void run()
         {
            process.destroy();
         }
      });

      ProcessLogger outputLogger = new ProcessLogger(true,
                                                     process.getInputStream(),
                                                     serverName,
                                                     false);
      outputLogger.start();

      // Adding a reader to System.err, so the VM won't hang on a System.err.println as identified on this forum thread:
      // http://www.jboss.org/index.html?module=bb&op=viewtopic&t=151815
      ProcessLogger errorLogger = new ProcessLogger(true,
                                                    process.getErrorStream(),
                                                    serverName,
                                                    true);
      errorLogger.start();

      // wait for start
      if (timeout != 0)
      {
         waitForServerToStart(id, timeout);
      }

      return process;
   }

   public static void waitForServerToStart(int id, int timeout) throws InterruptedException
   {
      long realTimeout = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < realTimeout)
      {
         try
         {
            HashMap<String, Object> params = new HashMap<String, Object>();
            params.put("host", "localhost");
            params.put("port", 61616 + id);
            TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
            ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
            cf.createConnection().close();
            System.out.println("server " + id + " started");
         }
         catch (Exception e)
         {
            System.out.println("awaiting server " + id + " start at " + (61616 + id));
            Thread.sleep(500);
            continue;
         }
         break;
      }
   }

   public static void killServer(final Process server) throws Exception
   {
      if (server != null)
      {
         System.out.println("**********************************");
         System.out.println("Killing server " + server);
         System.out.println("**********************************");
         server.destroy();
         server.waitFor();
         Thread.sleep(1000);
      }
   }

   public static int getServer(Connection connection)
   {
      ClientSession session = ((ActiveMQConnection) connection).getInitialSession();
      TransportConfiguration transportConfiguration = session.getSessionFactory().getConnectorConfiguration();
      String port = (String) transportConfiguration.getParams().get("port");
      return Integer.valueOf(port) - 61616;
   }

   public static  Connection getServerConnection(int server, Connection... connections)
   {
      for (Connection connection : connections)
      {
         ClientSession session = ((ActiveMQConnection) connection).getInitialSession();
         TransportConfiguration transportConfiguration = session.getSessionFactory().getConnectorConfiguration();
         String port = (String) transportConfiguration.getParams().get("port");
         if (Integer.valueOf(port) == server + 61616)
         {
            return connection;
         }
      }
      return null;
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
                     System.err.println(logName + "-err:" + line);
                  }
                  else
                  {
                     System.out.println(logName + "-out:" + line);
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
