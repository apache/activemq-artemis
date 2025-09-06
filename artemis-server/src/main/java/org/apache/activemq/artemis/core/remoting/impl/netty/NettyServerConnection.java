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
package org.apache.activemq.artemis.core.remoting.impl.netty;

import java.util.Map;

import io.netty.channel.Channel;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ServerConnectionLifeCycleListener;

import static org.apache.activemq.artemis.utils.ProxyProtocolUtil.PROXY_PROTOCOL_DESTINATION_ADDRESS;
import static org.apache.activemq.artemis.utils.ProxyProtocolUtil.PROXY_PROTOCOL_DESTINATION_PORT;
import static org.apache.activemq.artemis.utils.ProxyProtocolUtil.PROXY_PROTOCOL_SOURCE_ADDRESS;
import static org.apache.activemq.artemis.utils.ProxyProtocolUtil.PROXY_PROTOCOL_SOURCE_PORT;
import static org.apache.activemq.artemis.utils.ProxyProtocolUtil.PROXY_PROTOCOL_VERSION;

public class NettyServerConnection extends NettyConnection {

   private String sniHostname;

   private final String router;

   public NettyServerConnection(Map<String, Object> configuration,
                                Channel channel,
                                ServerConnectionLifeCycleListener listener,
                                boolean batchingEnabled,
                                boolean directDeliver,
                                String router) {
      super(configuration, channel, listener, batchingEnabled, directDeliver);

      this.router = router;
   }

   @Override
   public String getSNIHostName() {
      return sniHostname;
   }

   public void setSNIHostname(String sniHostname) {
      this.sniHostname = sniHostname;
   }

   @Override
   public String getRouter() {
      return router;
   }

   /**
    * {@return a string representation of the remote address of this connection; if this connection was made via the
    * proxy protocol then this will be the original address, not the proxy address}
    */
   @Override
   public String getRemoteAddress() {
      String proxyProtocolSourceAddress = channel.attr(PROXY_PROTOCOL_SOURCE_ADDRESS).get();
      String proxyProtocolSourcePort = channel.attr(PROXY_PROTOCOL_SOURCE_PORT).get();
      if (proxyProtocolSourceAddress != null && !proxyProtocolSourceAddress.isEmpty() && proxyProtocolSourcePort != null && !proxyProtocolSourcePort.isEmpty()) {
         return proxyProtocolSourceAddress + ":" + proxyProtocolSourcePort;
      } else {
         return super.getRemoteAddress();
      }
   }

   /**
    * {@return if this connection is made via the proxy protocol then this will be the address of the proxy}
    */
   public String getProxyAddress() {
      String proxyProtocolDestinationAddress = channel.attr(PROXY_PROTOCOL_DESTINATION_ADDRESS).get();
      String proxyProtocolDestinationPort = channel.attr(PROXY_PROTOCOL_DESTINATION_PORT).get();
      if (proxyProtocolDestinationAddress != null && !proxyProtocolDestinationAddress.isEmpty() && proxyProtocolDestinationPort != null && !proxyProtocolDestinationPort.isEmpty()) {
         return proxyProtocolDestinationAddress + ":" + proxyProtocolDestinationPort;
      } else {
         return null;
      }
   }

   /**
    * {@return the version of the proxy protocol used to make the connection or null if not applicable}
    */
   public String getProxyVersion() {
      return channel.attr(PROXY_PROTOCOL_VERSION).get() == null ? null : channel.attr(PROXY_PROTOCOL_VERSION).get().toString();
   }

   public static String getProxyAddress(Connection connection) {
      if (connection instanceof NettyServerConnection nettyServerConnection) {
         return nettyServerConnection.getProxyAddress();
      } else {
         return null;
      }
   }

   public static String getProxyVersion(Connection connection) {
      if (connection instanceof NettyServerConnection nettyServerConnection) {
         return nettyServerConnection.getProxyVersion();
      } else {
         return null;
      }
   }
}
