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
package org.apache.activemq.jms.tests.tools;

import java.net.UnknownHostException;

import org.jnp.server.Main;
import org.jnp.server.NamingBean;

/**
 * A WrappedJNDIServer
 *
 * We wrap the JBoss AS JNDI server, since we want to introduce a pause of 500 milliseconds on stop()
 *
 * This is because there is a bug in the JBoss AS class whereby the socket can remaining open some time after
 * stop() is called.
 *
 * So if you call stop() then start() quickly after, you can hit an  exception:
 *
 * java.rmi.server.ExportException: Port already in use: 1098; nested exception is:
 * java.net.BindException: Address already in use
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class WrappedJNDIServer
{
   private final Main main;

   public WrappedJNDIServer()
   {
      main = new Main();
   }

   public void start() throws Exception
   {
      main.start();
   }

   public void stop()
   {
      main.stop();

      try
      {
         Thread.sleep(500);
      }
      catch (Exception e)
      {
      }
   }

   public void setNamingInfo(final NamingBean naming)
   {
      main.setNamingInfo(naming);
   }

   public void setPort(final int port)
   {
      main.setPort(port);
   }

   public void setBindAddress(final String bindAddress) throws UnknownHostException
   {
      main.setBindAddress(bindAddress);
   }

   public void setRmiPort(final int port)
   {
      main.setRmiPort(port);
   }

   public void setRmiBindAddress(final String address) throws UnknownHostException
   {
      main.setRmiBindAddress(address);
   }
}
