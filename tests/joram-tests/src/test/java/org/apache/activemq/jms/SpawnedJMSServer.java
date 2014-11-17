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
package org.apache.activemq.jms;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Hashtable;

import javax.naming.InitialContext;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.jnp.server.Main;
import org.jnp.server.NamingBeanImpl;

/**
 * A SpawnedServer
 *
 * @author jmesnil
 *
 */
public class SpawnedJMSServer
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {
      try
      {
         System.setProperty("java.rmi.server.hostname", "localhost");
         System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");

         final NamingBeanImpl namingInfo = new NamingBeanImpl();
         namingInfo.start();
         final Main jndiServer = new Main();
         jndiServer.setNamingInfo(namingInfo);
         jndiServer.setPort(1099);
         jndiServer.setBindAddress("localhost");
         jndiServer.setRmiPort(1098);
         jndiServer.setRmiBindAddress("localhost");
         jndiServer.start();

         Configuration conf = new ConfigurationImpl()
            .addAcceptorConfiguration(new TransportConfiguration(NettyAcceptorFactory.class.getName()))
            .setSecurityEnabled(false)
            .setFileDeploymentEnabled(false);

         conf.getConnectorConfigurations().put("netty", new TransportConfiguration(NettyConnectorFactory.class.getName()));

         // disable server persistence since JORAM tests do not restart server
         final HornetQServer server = HornetQServers.newHornetQServer(conf, false);

         Hashtable<String, String> env = new Hashtable<String, String>();
         env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         env.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         JMSServerManager serverManager = new JMSServerManagerImpl(server);
         serverManager.setContext(new InitialContext(env));
         serverManager.start();

         System.out.println("Server started, ready to start client test");

         // create the reader before printing OK so that if the test is quick
         // we will still capture the STOP message sent by the client
         InputStreamReader isr = new InputStreamReader(System.in);
         BufferedReader br = new BufferedReader(isr);

         System.out.println("OK");

         String line = null;
         while ((line = br.readLine()) != null)
         {
            if ("STOP".equals(line.trim()))
            {
               server.stop();
               jndiServer.stop();
               namingInfo.stop();
               System.out.println("Server stopped");
               System.exit(0);
            }
            else
            {
               // stop anyway but with a error status
               System.exit(1);
            }
         }
      }
      catch (Throwable t)
      {
         t.printStackTrace();
         String allStack = t.getCause().getMessage() + "|";
         StackTraceElement[] stackTrace = t.getCause().getStackTrace();
         for (StackTraceElement stackTraceElement : stackTrace)
         {
            allStack += stackTraceElement.toString() + "|";
         }
         System.out.println(allStack);
         System.out.println("KO");
         System.exit(1);
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
