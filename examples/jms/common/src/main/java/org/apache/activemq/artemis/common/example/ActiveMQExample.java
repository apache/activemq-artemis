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

import java.io.File;
import java.util.HashMap;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.client.impl.DelegatingSession;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * Base class for ActiveMQ Artemis examples.
 * <p>
 * This takes care of starting and stopping the server as well as deploying any
 * queue needed.
 */
public abstract class ActiveMQExample
{
   protected static final Logger log = Logger.getLogger(ActiveMQExample.class.getName());

   protected boolean failure = false;

   protected String[] args;

   public abstract boolean runExample() throws Exception;

   protected void run(final String[] args1)
   {
      this.args = args1;
      //if we have a cluster of servers wait a while for the cluster to form properly
      if(args != null && args.length > 1)
      {
         System.out.println("****pausing to allow cluster to form****");
         Thread.yield();
         try
         {
            Thread.sleep(2000);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
      }

      try
      {
         if (!runExample())
         {
            failure = true;
         }
         System.out.println("example complete");
      }
      catch (Throwable e)
      {
         failure = true;
         e.printStackTrace();
      }
      reportResultAndExit();
   }

   protected void killServer(final int id) throws Exception
   {
      String configDir = System.getProperty("exampleConfigDir");
      if(configDir == null)
      {
         throw new Exception("exampleConfigDir must be set as a system property");
      }

      System.out.println("Killing server " + id);

      // We kill the server by creating a new file in the server dir which is checked for by the server
      // We can't use Process.destroy() since this does not do a hard kill - it causes shutdown hooks
      // to be called which cleanly shutdown the server
      System.out.println(configDir + "/server" + id + "/KILL_ME");
      File file = new File(configDir + "/server" + id + "/KILL_ME");

      file.createNewFile();

      // Sleep longer than the KillChecker check period
      Thread.sleep(3000);
   }

   protected void killServer(final int id, final int serverToWaitFor) throws Exception
   {
      String configDir = System.getProperty("exampleConfigDir");
      if(configDir == null)
      {
         throw new Exception("exampleConfigDir must be set as a system property");
      }

      System.out.println("Killing server " + id);

      // We kill the server by creating a new file in the server dir which is checked for by the server
      // We can't use Process.destroy() since this does not do a hard kill - it causes shutdown hooks
      // to be called which cleanly shutdown the server
      System.out.println(configDir + "/server" + id + "/KILL_ME");
      File file = new File(configDir + "/server" + id + "/KILL_ME");

      file.createNewFile();

      waitForServerStart(serverToWaitFor, 20000);
   }

   protected void reStartServer(final int id, final long timeout) throws Exception
   {
      String configDir = System.getProperty("exampleConfigDir");
      if(configDir == null)
      {
         throw new Exception("exampleConfigDir must be set as a system property");
      }

      System.out.println("restarting server " + id);

      // We kill the server by creating a new file in the server dir which is checked for by the server
      // We can't use Process.destroy() since this does not do a hard kill - it causes shutdown hooks
      // to be called which cleanly shutdown the server
      File file = new File(configDir + "/server" + id + "/RESTART_ME");

      file.createNewFile();

      waitForServerStart(id, timeout);

   }

   private void waitForServerStart(int id, long timeout) throws InterruptedException
   {
      // wait for restart
      long time = System.currentTimeMillis();
      while (time < System.currentTimeMillis() + timeout)
      {
         try
         {
            HashMap<String, Object> params = new HashMap<String, Object>();
            params.put("host", "localhost");
            params.put("port", 61616 + id);
            TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), params);
            ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
            cf.createConnection().close();
            System.out.println("server restarted");
         }
         catch (Exception e)
         {
            System.out.println("awaiting server restart");
            Thread.sleep(1000);
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
      return Integer.valueOf(port) - 61616;
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

   protected static final void closeConnection(final Connection connection) throws JMSException
   {
      if (connection != null)
      {
         connection.close();
      }
   }
}
