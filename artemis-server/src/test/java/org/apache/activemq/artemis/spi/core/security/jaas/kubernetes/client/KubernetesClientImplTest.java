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
package org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.client;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModuleTest.AUTH_JSON;
import static org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModuleTest.UNAUTH_JSON;
import static org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModuleTest.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import java.net.URL;

import org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.model.TokenReview;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.socket.PortFactory;
import org.mockserver.verify.VerificationTimes;

public class KubernetesClientImplTest {

   private static final String API_PATH = "/apis/authentication.k8s.io/v1/tokenreviews";
   private static ClientAndServer mockServer;
   private static final String host = "localhost";
   private static String port;

   private static final String BOB_REQUEST = "{\"apiVersion\": \"authentication.k8s.io/v1\"," +
         "\"kind\": \"TokenReview\", \"spec\": {\"token\": \"bob_token\"}}";

   private static final String KERMIT_REQUEST = "{\"apiVersion\": \"authentication.k8s.io/v1\"," +
         "\"kind\": \"TokenReview\", \"spec\": {\"token\": \"kermit_token\"}}";

   @BeforeClass
   public static void startServer() {
      ConfigurationProperties.dynamicallyCreateCertificateAuthorityCertificate(true);
      ConfigurationProperties.directoryToSaveDynamicSSLCertificate("target/test-classes");
      ConfigurationProperties.proactivelyInitialiseTLS(true);

      mockServer = ClientAndServer.startClientAndServer(PortFactory.findFreePort());
      port = Integer.toString(mockServer.getPort());

      assertNotNull(mockServer);
      assertTrue(mockServer.isRunning());
      System.setProperty("KUBERNETES_SERVICE_HOST", host);
      System.setProperty("KUBERNETES_SERVICE_PORT", port);
      System.setProperty("KUBERNETES_TOKEN_PATH",
            KubernetesClientImplTest.class.getClassLoader().getResource("client_token").getPath());

      mockServer.when(
            request()
                  .withMethod("POST")
                  .withPath(API_PATH)
                  .withBody(json(BOB_REQUEST, MatchType.STRICT)))
            .respond(
                  response()
                        .withStatusCode(HTTP_CREATED)
                        .withBody(UNAUTH_JSON));

      mockServer.when(
            request()
                  .withMethod("POST")
                  .withPath(API_PATH)
                  .withBody(json(KERMIT_REQUEST, MatchType.STRICT)))
            .respond(
                  response()
                        .withStatusCode(HTTP_CREATED)
                        .withBody(AUTH_JSON));

      mockServer.when(
            request()
                  .withMethod("POST")
                  .withPath(API_PATH))
            .respond(
                  response()
                        .withStatusCode(HTTP_INTERNAL_ERROR));


      // proactivelyInitialiseTLS to dynamicallyCreateCertificateAuthorityCertificate
      // only kicks in when the client is created to support the mock responses
      URL caPath = KubernetesClientImplTest.class.getClassLoader()
         .getResource("CertificateAuthorityCertificate.pem");
      assertNotNull(caPath);
      System.setProperty("KUBERNETES_CA_PATH", caPath.getPath());
   }

   @AfterClass
   public static void stopServer() {
      mockServer.stop();
   }

   @Test
   public void testGetTokenReview() {

      KubernetesClient client = new KubernetesClientImpl();

      TokenReview tr = client.getTokenReview("bob_token");
      assertNotNull(tr);
      assertFalse(tr.isAuthenticated());
      assertNull(tr.getUser());
      assertNull(tr.getUsername());

      tr = client.getTokenReview("kermit_token");
      assertNotNull(tr);
      assertNotNull(tr.getUser());
      assertThat(tr.getUsername(), is(USERNAME));
      assertThat(tr.getUser().getUsername(), is(USERNAME));

      tr = client.getTokenReview("other");
      assertNotNull(tr);
      assertFalse(tr.isAuthenticated());

      mockServer.verify(request().withPath(API_PATH), VerificationTimes.exactly(3));

   }

}
