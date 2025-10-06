/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.remoting;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.security.Principal;
import java.security.cert.X509Certificate;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;

public class CertificateUtil {

   private static final String SSL_HANDLER_NAME = "ssl";

   public static String getCertSubjectDN(RemotingConnection connection) {
      String certSubjectDN = "unavailable";
      X509Certificate[] certs = getCertsFromConnection(connection);
      if (certs != null && certs.length > 0 && certs[0] != null) {
         certSubjectDN = certs[0].getSubjectDN().getName();
      }
      return certSubjectDN;
   }

   public static X509Certificate[] getCertsFromConnection(RemotingConnection remotingConnection) {
      X509Certificate[] certificates = null;
      if (remotingConnection != null && remotingConnection.getTransportConnection() instanceof NettyConnection nettyConnection) {
         certificates = nettyConnection.getCertificates();
      }
      return certificates;
   }

   public static Principal getPeerPrincipalFromConnection(RemotingConnection remotingConnection) {
      Principal result = null;
      if (remotingConnection != null) {
         Connection transportConnection = remotingConnection.getTransportConnection();
         if (transportConnection instanceof NettyConnection nettyConnection) {
            ChannelHandler channelHandler = nettyConnection.getChannel().pipeline().get(SSL_HANDLER_NAME);
            if (channelHandler != null && channelHandler instanceof SslHandler sslHandler) {
               try {
                  result = sslHandler.engine().getSession().getPeerPrincipal();
               } catch (SSLPeerUnverifiedException ignored) {
               }
            }
         }
      }

      return result;
   }

   public static Principal getLocalPrincipalFromConnection(NettyConnection nettyConnection) {
      Principal result = null;
      ChannelHandler handler = nettyConnection.getChannel().pipeline().get(SSL_HANDLER_NAME);
      if (handler instanceof SslHandler sslHandler) {
         result = sslHandler.engine().getSession().getLocalPrincipal();
      }

      return result;
   }
}
