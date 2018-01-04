/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Test;

public class MQTTSecurityCRLTest extends ActiveMQTestBase {
   /**
    * These artifacts are required for testing mqtt with CRL
    * <p>
    * openssl genrsa -out ca.key 2048
    * openssl req -new -x509 -days 1826 -key ca.key -out ca.crt
    * touch certindex
    * echo 01 > certserial
    * echo 01 > crlnumber
    * <p>
    * Create ca.conf file with
    * <p>
    * [ ca ]
    * default_ca = myca
    * <p>
    * [ crl_ext ]
    * # issuerAltName=issuer:copy #this would copy the issuer name to altname
    * authorityKeyIdentifier=keyid:always
    * <p>
    * [ myca ]
    * dir = ./
    * new_certs_dir = $dir
    * unique_subject = no
    * certificate = $dir/ca.crt
    * database = $dir/certindex
    * private_key = $dir/ca.key
    * serial = $dir/certserial
    * default_days = 730
    * default_md = sha1
    * policy = myca_policy
    * x509_extensions = myca_extensions
    * crlnumber = $dir/crlnumber
    * default_crl_days = 730
    * <p>
    * [ myca_policy ]
    * commonName = supplied
    * stateOrProvinceName = supplied
    * countryName = optional
    * emailAddress = optional
    * organizationName = supplied
    * organizationalUnitName = optional
    * <p>
    * [ myca_extensions ]
    * basicConstraints = CA:false
    * subjectKeyIdentifier = hash
    * authorityKeyIdentifier = keyid:always
    * keyUsage = digitalSignature,keyEncipherment
    * extendedKeyUsage = serverAuth, clientAuth
    * crlDistributionPoints = URI:http://example.com/root.crl
    * subjectAltName = @alt_names
    * <p>
    * [alt_names]
    * DNS.1 = example.com
    * DNS.2 = *.example.com
    * <p>
    * Continue executing the commands:
    * <p>
    * openssl genrsa -out keystore1.key 2048
    * openssl req -new -key keystore1.key -out keystore1.csr
    * openssl ca -batch -config ca.conf -notext -in keystore1.csr -out keystore1.crt
    * openssl genrsa -out client_revoked.key 2048
    * openssl req -new -key client_revoked.key -out client_revoked.csr
    * openssl ca -batch -config ca.conf -notext -in client_revoked.csr -out client_revoked.crt
    * openssl genrsa -out client_not_revoked.key 2048
    * openssl req -new -key client_not_revoked.key -out client_not_revoked.csr
    * openssl ca -batch -config ca.conf -notext -in client_not_revoked.csr -out client_not_revoked.crt
    * openssl ca -config ca.conf -gencrl -keyfile ca.key -cert ca.crt -out root.crl.pem
    * openssl ca -config ca.conf -revoke client_revoked.crt -keyfile ca.key -cert ca.crt
    * openssl ca -config ca.conf -gencrl -keyfile ca.key -cert ca.crt -out root.crl.pem
    * <p>
    * openssl pkcs12 -export -name client_revoked -in client_revoked.crt -inkey client_revoked.key -out client_revoked.p12
    * keytool -importkeystore -destkeystore client_revoked.jks -srckeystore client_revoked.p12 -srcstoretype pkcs12 -alias client_revoked
    * <p>
    * openssl pkcs12 -export -name client_not_revoked -in client_not_revoked.crt -inkey client_not_revoked.key -out client_not_revoked.p12
    * keytool -importkeystore -destkeystore client_not_revoked.jks -srckeystore client_not_revoked.p12 -srcstoretype pkcs12 -alias client_not_revoked
    * <p>
    * openssl pkcs12 -export -name keystore1 -in keystore1.crt -inkey keystore1.key -out keystore1.p12
    * keytool -importkeystore -destkeystore keystore1.jks -srckeystore keystore1.p12 -srcstoretype pkcs12 -alias keystore1
    * <p>
    * keytool -import -trustcacerts -alias trust_key -file ca.crt -keystore truststore.jks
    */

   @Test(expected = SSLException.class)
   public void crlRevokedTest() throws Exception {

      ActiveMQServer server1 = initServer();
      BlockingConnection connection1 = null;
      try {
         server1.start();

         while (!server1.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("ssl://localhost:1883", "truststore.jks", "changeit", "client_revoked.jks", "changeit");

         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);

         // Publish Messages
         String payload1 = "This is message 1";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);

         assertEquals(payload1, new String(message1.getPayload()));

      } finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }
   }

   @Test
   public void crlNotRevokedTest() throws Exception {

      ActiveMQServer server1 = initServer();
      BlockingConnection connection1 = null;
      try {
         server1.start();

         while (!server1.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("ssl://localhost:1883", "truststore.jks", "changeit", "client_not_revoked.jks", "changeit");

         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);

         // Publish Messages
         String payload1 = "This is message 1";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);

         assertEquals(payload1, new String(message1.getPayload()));

      } finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }
   }


   private ActiveMQServer initServer() throws Exception {
      Configuration configuration = createDefaultNettyConfig().setSecurityEnabled(false);

      addMqttTransportConfiguration(configuration);
      addWildCardConfiguration(configuration);

      ActiveMQServer server = createServer(true, configuration);
      return server;
   }

   private void addWildCardConfiguration(Configuration configuration) {
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setAnyWords('#');
      wildcardConfiguration.setDelimiter('/');
      wildcardConfiguration.setRoutingEnabled(true);
      wildcardConfiguration.setSingleWord('+');

      configuration.setWildCardConfiguration(wildcardConfiguration);
   }

   private void addMqttTransportConfiguration(Configuration configuration) throws IOException {
      TransportConfiguration transportConfiguration = new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(), null, "mqtt", null);

      transportConfiguration.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      transportConfiguration.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "truststore.jks");
      transportConfiguration.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "changeit");
      transportConfiguration.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "keystore1.jks");
      transportConfiguration.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "changeit");
      transportConfiguration.getParams().put(TransportConstants.CRL_PATH_PROP_NAME, "root.crl.pem");
      transportConfiguration.getParams().put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, "true");
      transportConfiguration.getParams().put(TransportConstants.PORT_PROP_NAME, "1883");
      transportConfiguration.getParams().put(TransportConstants.HOST_PROP_NAME, "localhost");
      transportConfiguration.getParams().put(TransportConstants.PROTOCOLS_PROP_NAME, "MQTT");

      configuration.getAcceptorConfigurations().add(transportConfiguration);
   }

   private BlockingConnection retrieveMQTTConnection(String host, String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(1);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setHost(host);
      SSLContext sslContext = SSLSupport.createContext(TransportConstants.DEFAULT_KEYSTORE_PROVIDER, keystorePath, keystorePass, TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER, truststorePath, truststorePass);
      mqtt.setSslContext(sslContext);

      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }


}
