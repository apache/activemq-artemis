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
package org.apache.activemq.jms.example;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.activemq.common.example.ActiveMQExample;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * A HttpServer
 *
 * * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * @author <a href="mailto:jmesnil@redhat.com>Jeff Mesnil</a>
 *
 *
 */
public class HttpStaticFileServer extends ActiveMQExample
{

   public static void main(final String[] args)
   {
      new HttpStaticFileServer().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      // Configure the server.
      ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                                        Executors.newCachedThreadPool()));
      // Set up the event pipeline factory.
      bootstrap.setPipelineFactory(new HttpStaticFileServerPipelineFactory());
      // Bind and start to accept incoming connections.
      bootstrap.bind(new InetSocketAddress(8088));

      System.out.println("HTTP server ready to server on 8088");

      System.out.println("open http://127.0.0.1:8088/applet.html to use the Applet");

      while (true)
      {
         Thread.sleep(100);
      }

   }

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
