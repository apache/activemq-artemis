/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

public class CertificateUtil {

   public static X509Certificate[] getCertsFromChannel(Channel channel) {
      X509Certificate[] certificates = null;
      ChannelHandler channelHandler = channel.pipeline().get("ssl");
      if (channelHandler != null && channelHandler instanceof SslHandler) {
         SslHandler sslHandler = (SslHandler) channelHandler;
         try {
            certificates = sslHandler.engine().getSession().getPeerCertificateChain();
         } catch (SSLPeerUnverifiedException e) {
            // ignore
         }
      }

      return certificates;
   }
}
