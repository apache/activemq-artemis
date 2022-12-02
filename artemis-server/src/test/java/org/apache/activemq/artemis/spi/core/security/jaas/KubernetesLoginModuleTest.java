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
package org.apache.activemq.artemis.spi.core.security.jaas;

import static org.apache.activemq.artemis.spi.core.security.jaas.KubernetesLoginModule.K8S_ROLE_FILE_PROP_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.TokenCallbackHandler;
import org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.client.KubernetesClient;
import org.apache.activemq.artemis.spi.core.security.jaas.kubernetes.model.TokenReview;
import org.junit.Test;

public class KubernetesLoginModuleTest {

   private final KubernetesClient client = mock(KubernetesClient.class);
   private final KubernetesLoginModule loginModule = new KubernetesLoginModule(client);
   private static final String TOKEN = "the_token";

   public static final String USERNAME = "system:serviceaccounts:some-ns:kermit";
   public static final String AUTH_JSON = "{\"status\": {"
         + "\"authenticated\": true, "
         + "\"user\": {"
         + "  \"username\": \"" + USERNAME + "\""
         + "}}}";

   public static final String UNAUTH_JSON = "{\"status\": {"
         + "\"authenticated\": false "
         + "}}";

   @Test
   public void testBasicLogin() throws LoginException {
      CallbackHandler handler = new TokenCallbackHandler(TOKEN);
      Subject subject = new Subject();
      loginModule.initialize(subject, handler, Collections.emptyMap(), getDefaultOptions());

      TokenReview tr = TokenReview.fromJsonString(AUTH_JSON);
      when(client.getTokenReview(TOKEN)).thenReturn(tr);

      assertTrue(loginModule.login());
      assertTrue(loginModule.commit());

      assertThat(subject.getPrincipals(UserPrincipal.class), hasSize(1));
      subject.getPrincipals(ServiceAccountPrincipal.class).forEach(p -> {
         assertThat(p.getName(), is(USERNAME));
         assertThat(p.getSaName(), is("kermit"));
         assertThat(p.getNamespace(), is("some-ns"));
      });
      Set<RolePrincipal> roles = subject.getPrincipals(RolePrincipal.class);
      assertThat(roles, hasSize(2));
      assertThat(roles, containsInAnyOrder(new RolePrincipal("muppet"), new RolePrincipal("admin")));

      assertTrue(loginModule.logout());
      assertFalse(loginModule.commit());
      assertThat(subject.getPrincipals(), empty());
      verify(client, times(1)).getTokenReview(TOKEN);
   }

   @Test
   public void testFailedLogin() throws LoginException {
      CallbackHandler handler = new TokenCallbackHandler(TOKEN);
      Subject subject = new Subject();
      loginModule.initialize(subject, handler, Collections.emptyMap(), getDefaultOptions());

      TokenReview tr = TokenReview.fromJsonString(UNAUTH_JSON);
      when(client.getTokenReview(TOKEN)).thenReturn(tr);

      assertFalse(loginModule.login());
      assertFalse(loginModule.commit());
      assertThat(subject.getPrincipals(), empty());
      verify(client, times(1)).getTokenReview(TOKEN);
   }

   @Test
   public void testNullToken() throws LoginException {
      CallbackHandler handler = new TokenCallbackHandler(null);
      Subject subject = new Subject();
      loginModule.initialize(subject, handler, Collections.emptyMap(), getDefaultOptions());

      try {
         assertFalse(loginModule.login());
         fail("Exception expected");
      } catch (LoginException e) {
         assertNotNull(e);
      }

      assertFalse(loginModule.commit());
      assertThat(subject.getPrincipals(), empty());
      verifyNoInteractions(client);
   }

   @Test
   public void testUnableToVerifyToken() throws LoginException {
      CallbackHandler handler = new TokenCallbackHandler(TOKEN);
      Subject subject = new Subject();
      loginModule.initialize(subject, handler, Collections.emptyMap(), getDefaultOptions());

      when(client.getTokenReview(TOKEN)).thenReturn(new TokenReview());

      assertFalse(loginModule.login());
      assertFalse(loginModule.commit());
      assertThat(subject.getPrincipals(), empty());
      verify(client, times(1)).getTokenReview(TOKEN);
   }

   private Map<String, ?> getDefaultOptions() {
      String baseDirValue = new File(KubernetesLoginModuleTest.class.getClassLoader().getResource("k8s-roles.properties").getPath()).getParentFile().getAbsolutePath();
      return Map.of(K8S_ROLE_FILE_PROP_NAME, "k8s-roles.properties", "baseDir",baseDirValue);
   }
}
