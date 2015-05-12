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
package org.apache.activemq.artemis.common.example;

import javax.jms.Connection;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * This cass is a base class for all the tests where we have a few utilities to start and stop servers *
 */
public abstract class ActiveMQExample
{
   protected static final Logger log = Logger.getLogger(ActiveMQExample.class.getName());
   public static final int DEFAULT_PORT = 61616;

   public static final String DEFAULT_TCP1 = "tcp://localhost:61616";
   public static final String DEFAULT_TCP2 = "tcp://localhost:61617";
   public static final String DEFAULT_TCP3 = "tcp://localhost:61618";
   public static final String DEFAULT_TCP4 = "tcp://localhost:61619";

   protected boolean failure = false;

   protected String[] serversArgs;

   protected Process[] processes;


   // This is to make sure we stop the servers when the example is shutdown
   // as we start the servers with the example
   Thread hook;

   public void run(String servers[])
   {
      try
      {
         setupServers(servers);
         if (!runExample())
         {
            failure = true;
         }
      }
      catch (Throwable throwable)
      {
         failure = true;
         throwable.printStackTrace();
      }
      finally
      {
         try
         {
            stopAllServers();
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }
      }

      reportResultAndExit();

   }
   public abstract boolean runExample() throws Exception;

   public void close() throws Exception
   {

      if (hook != null)
      {
         Runtime.getRuntime().removeShutdownHook(hook);
         hook = null;
      }
      stopAllServers();
   }

   /** This will start the servers */
   private final ActiveMQExample setupServers(String[] serversArgs) throws Exception
   {
      hook = new Thread()
      {
         public void run()
         {
            try
            {
               System.out.println("Shutting down servers!!!");
               System.out.flush();
               ActiveMQExample.this.stopAllServers();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      Runtime.getRuntime().addShutdownHook(hook);
      this.serversArgs = serversArgs;

      if (serversArgs == null)
      {
         serversArgs = new String[0];
      }

      processes = new Process[serversArgs.length];
      startServers(serversArgs);

      return this;
   }

   protected void startServers(String[] serversArgs) throws Exception
   {
      for (int i = 0; i < serversArgs.length; i++)
      {
         startServer(i, 5000);
      }
   }

   protected void stopAllServers() throws Exception
   {
      if (processes != null)
      {
         for (int i = 0; i < processes.length; i++)
         {
            killServer(i);
         }
      }
   }

   protected void killServer(final int id) throws Exception
   {
      if (id > processes.length)
      {
         System.out.println("**********************************");
         System.out.println("Kill server " + id + " manually, will wait 5 seconds for that being done");
         Thread.sleep(5000);
      }

      if (processes[id] != null)
      {
         System.out.println("**********************************");
         System.out.println("Kill server " + id);
         processes[id].destroyForcibly();
         processes[id].waitFor();
//         processes[id].destroy();
         processes[id] = null;
         Thread.sleep(1000);
      }
   }

   protected void reStartServer(final int id, final long timeout) throws Exception
   {
      startServer(id, timeout);
   }

   protected void startServer(final int id, final long timeout) throws Exception
   {
      if (id > processes.length)
      {
         System.out.println("**********************************");
         System.out.println("Start server " + id + " manually");
         Thread.sleep(5000);
      }
      else
      {
         // if started before, will kill it
         if (processes[id] != null)
         {
            killServer(id);
         }
      }

      processes[id] = ExampleUtil.startServer(serversArgs[id], "Server_" + id);

      if (timeout != 0)
      {
         waitForServerStart(id, timeout);
      }
      else
      {
         // just some time wait to wait server to form clusters.. etc
         Thread.sleep(500);
      }
   }

   protected void waitForServerStart(int id, long timeoutParameter) throws InterruptedException
   {
      // wait for restart
      long timeout = System.currentTimeMillis() + timeoutParameter;
      while (System.currentTimeMillis() < timeout)
      {
         try
         {
            HashMap<String, Object> params = new HashMap<String, Object>();
            params.put("host", "localhost");
            params.put("port", DEFAULT_PORT + id);
            TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
            ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
            cf.createConnection().close();
            System.out.println("server " + id + " started");
         }
         catch (Exception e)
         {
            System.out.println("awaiting server " + id + " start at " + (DEFAULT_PORT + id));
            Thread.sleep(500);
            continue;
         }
         break;
      }
   }

   protected int getServer(Connection connection)
   {
      ClientSession session = ((ActiveMQConnection) connection).getInitialSession();
      TransportConfiguration transportConfiguration = session.getSessionFactory().getConnectorConfiguration();
      String port = (String) transportConfiguration.getParams().get("port");
      return Integer.valueOf(port) - DEFAULT_PORT;
   }

   protected Connection getServerConnection(int server, Connection... connections)
   {
      for (Connection connection : connections)
      {
         ClientSession session = ((ActiveMQConnection) connection).getInitialSession();
         TransportConfiguration transportConfiguration = session.getSessionFactory().getConnectorConfiguration();
         String port = (String) transportConfiguration.getParams().get("port");
         if(Integer.valueOf(port) == server + 61616)
         {
            return connection;
         }
      }
      return null;
   }

   private void reportResultAndExit()
   {
      if (failure)
      {
         System.err.println();
         System.err.println("#####################");
         System.err.println("###    FAILURE!   ###");
         System.err.println("#####################");
         throw new RuntimeException("failure in running example");
      }
      else
      {
         System.out.println();
         System.out.println("#####################");
         System.out.println("###    SUCCESS!   ###");
         System.out.println("#####################");
      }
   }
}
