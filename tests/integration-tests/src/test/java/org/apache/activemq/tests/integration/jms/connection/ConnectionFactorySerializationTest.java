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
package org.apache.activemq.tests.integration.jms.connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Justin Bertram
 */
public class ConnectionFactorySerializationTest extends JMSTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static ActiveMQConnectionFactory cf;

   // Constructors --------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception
   {
      try
      {
         super.setUp();
         // Deploy a connection factory with discovery
         List<String> bindings = new ArrayList<String>();
         bindings.add("MyConnectionFactory");
         final String groupAddress = getUDPDiscoveryAddress();
         final int port = getUDPDiscoveryPort();
         String localBindAddress = getLocalHost().getHostAddress();

         UDPBroadcastGroupConfiguration config = new UDPBroadcastGroupConfiguration()
            .setGroupAddress(groupAddress)
            .setGroupPort(port)
            .setLocalBindAddress(localBindAddress)
            .setLocalBindPort(8580);

         DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration()
            .setName("dg1")
            .setRefreshTimeout(5000)
            .setDiscoveryInitialWaitTimeout(5000)
            .setBroadcastEndpointFactoryConfiguration(config);

         jmsServer.getActiveMQServer().getConfiguration().getDiscoveryGroupConfigurations().put(dcConfig.getName(), dcConfig);

         jmsServer.createConnectionFactory("MyConnectionFactory",
                                           false,
                                           JMSFactoryType.CF,
                                           dcConfig.getName(),
                                           "/MyConnectionFactory");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   // Public --------------------------------------------------------

   @Test
   public void testNullLocalBindAddress() throws Exception
   {
      cf = (ActiveMQConnectionFactory) namingContext.lookup("/MyConnectionFactory");

      // apparently looking up the connection factory with the org.apache.activemq.jms.tests.tools.container.InVMInitialContextFactory
      // is not enough to actually serialize it so we serialize it manually
      byte[] x = serialize(cf);
      ActiveMQConnectionFactory y = deserialize(x, ActiveMQConnectionFactory.class);
      Assert.assertEquals(null, ((UDPBroadcastGroupConfiguration) y.getDiscoveryGroupConfiguration().getBroadcastEndpointFactoryConfiguration()).getLocalBindAddress());
   }

   private static <T extends Serializable> byte[] serialize(T obj) throws IOException
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      return baos.toByteArray();
   }

   private static <T extends Serializable> T deserialize(byte[] b, Class<T> cl) throws IOException, ClassNotFoundException
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      ObjectInputStream ois = new ObjectInputStream(bais);
      Object o = ois.readObject();
      return cl.cast(o);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected static InetAddress getLocalHost() throws UnknownHostException
   {
      InetAddress addr;
      try
      {
         addr = InetAddress.getLocalHost();
      }
      catch (ArrayIndexOutOfBoundsException e)
      {  //this is workaround for mac osx bug see AS7-3223 and JGRP-1404
         addr = InetAddress.getByName(null);
      }
      return addr;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
