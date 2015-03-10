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
package org.apache.activemq.jms;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Hashtable;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientRequestor;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.ManagementHelper;
import org.apache.activemq.api.core.management.ResourceNames;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.tests.util.SpawnedVMSupport;
import org.junit.Assert;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * A ActiveMQAdmin
 */
public class ActiveMQAdmin implements Admin
{

   private ClientSession clientSession;

   private ClientRequestor requestor;

   private Context context;

   private Process serverProcess;

   private ClientSessionFactory sf;

   ServerLocator serverLocator;
   /**
    * Determines whether to act or 'no-op' on {@link ActiveMQAdmin#serverStart()} and
    * {@link ActiveMQAdmin#serverStop()}. This is used when testing combinations of client and
    * servers with different versions.
    *
    * @see https://github.com/activemq/activemq-version-tests
    */
   private final boolean serverLifeCycleActive;
   private static final String SERVER_LIVE_CYCLE_PROPERTY = "org.apache.activemq.jms.ActiveMQAdmin.serverLifeCycle";

   public ActiveMQAdmin()
   {
      serverLifeCycleActive = Boolean.valueOf(System.getProperty(SERVER_LIVE_CYCLE_PROPERTY, "true"));
      try
      {
         Hashtable<String, String> env = new Hashtable<String, String>();
         env.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
         env.put("java.naming.provider.url", "tcp://localhost:61616");
         context = new InitialContext(env);
      }
      catch (NamingException e)
      {
         e.printStackTrace();
      }
   }

   public void start() throws Exception
   {
      serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      sf = serverLocator.createSessionFactory();
      clientSession = sf.createSession(ActiveMQDefaultConfiguration.getDefaultClusterUser(),
                                       ActiveMQDefaultConfiguration.getDefaultClusterPassword(),
                                       false,
                                       true,
                                       true,
                                       false,
                                       1);
      requestor = new ClientRequestor(clientSession, ActiveMQDefaultConfiguration.getDefaultManagementAddress());
      clientSession.start();

   }

   public void stop() throws Exception
   {
      requestor.close();

      if (sf != null)
      {
         sf.close();
      }

      if (serverLocator != null)
      {
         serverLocator.close();
      }

      sf = null;
      serverLocator = null;
   }

   public void createConnectionFactory(final String name)
   {
      createConnection(name, 0);
   }

   private void createConnection(final String name, final int cfType)
   {
      try
      {
         invokeSyncOperation(ResourceNames.JMS_SERVER,
                             "createConnectionFactory",
                             name,
                             false,
                             false,
                             cfType,
                             "netty",
                             name);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }

   }

   public Context createContext() throws NamingException
   {
      return context;
   }

   public void createQueue(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean) invokeSyncOperation(ResourceNames.JMS_SERVER, "createQueue", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createQueueConnectionFactory(final String name)
   {
      createConnection(name, 1);
   }

   public void createTopic(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean) invokeSyncOperation(ResourceNames.JMS_SERVER, "createTopic", name, name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void createTopicConnectionFactory(final String name)
   {
      createConnection(name, 2);
   }

   public void deleteConnectionFactory(final String name)
   {
      try
      {
         invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyConnectionFactory", name);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueue(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean) invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyQueue", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteQueueConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public void deleteTopic(final String name)
   {
      Boolean result;
      try
      {
         result = (Boolean) invokeSyncOperation(ResourceNames.JMS_SERVER, "destroyTopic", name);
         Assert.assertEquals(true, result.booleanValue());
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public void deleteTopicConnectionFactory(final String name)
   {
      deleteConnectionFactory(name);
   }

   public String getName()
   {
      return this.getClass().getName();
   }

   public void startServer() throws Exception
   {
      if (!serverLifeCycleActive)
      {
         return;
      }

      String[] vmArgs = new String[]{};
      serverProcess = SpawnedVMSupport.spawnVM(SpawnedJMSServer.class.getName(), vmArgs, false);
      InputStreamReader isr = new InputStreamReader(serverProcess.getInputStream());

      final BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null)
      {
         System.out.println("SERVER: " + line);
         line.replace('|', '\n');
         if ("OK".equals(line.trim()))
         {
            new Thread()
            {
               @Override
               public void run()
               {
                  try
                  {
                     String line1 = null;
                     while ((line1 = br.readLine()) != null)
                     {
                        System.out.println("SERVER: " + line1);
                     }
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            }.start();
            return;
         }
         else if ("KO".equals(line.trim()))
         {
            // something went wrong with the server, destroy it:
            serverProcess.destroy();
            throw new IllegalStateException("Unable to start the spawned server :" + line);
         }
      }
   }

   public void stopServer() throws Exception
   {
      if (!serverLifeCycleActive)
      {
         return;
      }
      OutputStreamWriter osw = new OutputStreamWriter(serverProcess.getOutputStream());
      osw.write("STOP\n");
      osw.flush();
      int exitValue = serverProcess.waitFor();
      if (exitValue != 0)
      {
         serverProcess.destroy();
      }
   }

   private Object invokeSyncOperation(final String resourceName, final String operationName, final Object... parameters) throws Exception
   {
      ClientMessage message = clientSession.createMessage(false);
      ManagementHelper.putOperationInvocation(message, resourceName, operationName, parameters);
      ClientMessage reply;
      try
      {
         reply = requestor.request(message, 3000);
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Exception while invoking " + operationName + " on " + resourceName, e);
      }
      if (reply == null)
      {
         throw new IllegalStateException("no reply received when invoking " + operationName + " on " + resourceName);
      }
      if (!ManagementHelper.hasOperationSucceeded(reply))
      {
         throw new IllegalStateException("operation failed when invoking " + operationName +
                                            " on " +
                                            resourceName +
                                            ": " +
                                            ManagementHelper.getResult(reply));
      }
      return ManagementHelper.getResult(reply);
   }

   // Inner classes -------------------------------------------------

}
