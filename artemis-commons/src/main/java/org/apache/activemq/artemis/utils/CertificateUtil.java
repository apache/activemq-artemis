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
import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import org.jboss.logging.Logger;

public class CertificateUtil {
   private static final Logger logger = Logger.getLogger(CertificateUtil.class);

   public static X509Certificate[] getCertsFromChannel(Channel channel) {
      Certificate[] plainCerts = null;
      ChannelHandler channelHandler = channel.pipeline().get("ssl");
      if (channelHandler != null && channelHandler instanceof SslHandler) {
         SslHandler sslHandler = (SslHandler) channelHandler;
         try {
            plainCerts = sslHandler.engine().getSession().getPeerCertificates();
         } catch (SSLPeerUnverifiedException e) {
            // ignore
         }
      }

      X509Certificate[] x509Certs = null;
      if (plainCerts != null && plainCerts.length > 0) {
         x509Certs = new X509Certificate[plainCerts.length];
         for (int i = 0; i < plainCerts.length; i++) {
            if (plainCerts[i] instanceof X509Certificate) {
               x509Certs[i] = (X509Certificate) plainCerts[i];
            } else {
               try {
                  x509Certs[i] = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(plainCerts[i].getEncoded()));
               } catch (Exception ex) {
                  if (logger.isTraceEnabled()) {
                     logger.trace("Failed to convert SSL cert", ex);
                  }
                  return null;
               }
            }
            if (logger.isTraceEnabled()) {
               logger.trace("Cert #" + i + " = " + x509Certs[i]);
            }
         }
      }

      return x509Certs;
   }
}
