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
package org.apache.activemq.tests.unit.core.remoting.impl.netty;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Assert;

import org.apache.activemq.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.spi.core.remoting.BufferHandler;
import org.apache.activemq.spi.core.remoting.Connection;
import org.apache.activemq.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.tests.util.UnitTestCase;

public class NettyConnectorTest extends UnitTestCase
{

   @Test
   public void testStartStop() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
                                                    handler,
                                                    listener,
                                                    Executors.newCachedThreadPool(),
                                                    Executors.newCachedThreadPool(),
                                                    Executors.newScheduledThreadPool(5));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testNullParams() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }

         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      try
      {
         new NettyConnector(params,
                            null,
                            listener,
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool(),
                            Executors.newScheduledThreadPool(5));

         Assert.fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         // Ok
      }

      try
      {
         new NettyConnector(params,
                            handler,
                            null,
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool(),
                            Executors.newScheduledThreadPool(5));

         Assert.fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         // Ok
      }
   }
   @Test
   public void testJavaSystemPropertyOverrides() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
            handler,
            listener,
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newScheduledThreadPool(5));


      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }
   @Test
   public void testActiveMQSystemPropertyOverrides() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
            handler,
            listener,
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newScheduledThreadPool(5));


      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "bad path");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");

      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      System.setProperty(NettyConnector.ACTIVEMQ_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");


      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testBadCipherSuite() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, "myBadCipherSuite");
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
            handler,
            listener,
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newScheduledThreadPool(5));


      connector.start();
      Assert.assertTrue(connector.isStarted());
      Assert.assertNull(connector.createConnection());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   @Test
   public void testBadProtocol() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "myBadProtocol");
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
            handler,
            listener,
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newScheduledThreadPool(5));


      connector.start();
      Assert.assertTrue(connector.isStarted());
      Assert.assertNull(connector.createConnection());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }
}
