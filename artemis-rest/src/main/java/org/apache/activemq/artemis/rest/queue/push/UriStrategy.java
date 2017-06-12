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
package org.apache.activemq.artemis.rest.queue.push;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.xml.BasicAuth;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.XmlHttpHeader;
import org.apache.activemq.artemis.rest.util.HttpMessageHelper;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.core.executors.ApacheHttpClient4Executor;
import org.jboss.resteasy.specimpl.ResteasyUriBuilder;

public class UriStrategy implements PushStrategy {

   ThreadSafeClientConnManager connManager = new ThreadSafeClientConnManager();
   protected HttpClient client = new DefaultHttpClient(connManager);
   protected BasicHttpContext localContext;
   protected ApacheHttpClient4Executor executor = new ApacheHttpClient4Executor(client);
   protected PushRegistration registration;
   protected UriBuilder targetUri;
   protected String method;
   protected String contentType;

   protected ConnectionFactoryOptions jmsOptions;

   UriStrategy() {
      connManager.setDefaultMaxPerRoute(100);
      connManager.setMaxTotal(1000);
   }

   @Override
   public void setRegistration(PushRegistration reg) {
      this.registration = reg;
   }

   @Override
   public void start() throws Exception {
      initAuthentication();
      method = registration.getTarget().getMethod();
      if (method == null)
         method = "POST";
      contentType = registration.getTarget().getType();
      targetUri = ResteasyUriBuilder.fromTemplate(registration.getTarget().getHref());
   }

   protected void initAuthentication() {
      if (registration.getAuthenticationMechanism() != null) {
         if (registration.getAuthenticationMechanism().getType() instanceof BasicAuth) {
            BasicAuth basic = (BasicAuth) registration.getAuthenticationMechanism().getType();
            UsernamePasswordCredentials creds = new UsernamePasswordCredentials(basic.getUsername(), basic.getPassword());
            AuthScope authScope = new AuthScope(AuthScope.ANY);
            ((DefaultHttpClient) client).getCredentialsProvider().setCredentials(authScope, creds);

            localContext = new BasicHttpContext();

            // Generate BASIC scheme object and stick it to the local execution context
            BasicScheme basicAuth = new BasicScheme();
            localContext.setAttribute("preemptive-auth", basicAuth);

            // Add as the first request interceptor
            ((DefaultHttpClient) client).addRequestInterceptor(new PreemptiveAuth(), 0);
            executor.setHttpContext(localContext);
         }
      }
   }

   @Override
   public void stop() {
      connManager.shutdown();
   }

   @Override
   public void setJmsOptions(ConnectionFactoryOptions jmsOptions) {
      this.jmsOptions = jmsOptions;
   }

   @Override
   public boolean push(ClientMessage message) {
      ActiveMQRestLogger.LOGGER.debug("Pushing " + message);
      String uri = createUri(message);
      for (int i = 0; i < registration.getMaxRetries(); i++) {
         long wait = registration.getRetryWaitMillis();
         System.out.println("Creating request from " + uri);
         ClientRequest request = executor.createRequest(uri);
         request.followRedirects(false);
         ActiveMQRestLogger.LOGGER.debug("Created request " + request);

         for (XmlHttpHeader header : registration.getHeaders()) {
            ActiveMQRestLogger.LOGGER.debug("Setting XmlHttpHeader: " + header.getName() + "=" + header.getValue());
            request.header(header.getName(), header.getValue());
         }
         HttpMessageHelper.buildMessage(message, request, contentType, jmsOptions);
         ClientResponse<?> res = null;
         try {
            ActiveMQRestLogger.LOGGER.debug(method + " " + uri);
            res = request.httpMethod(method);
            int status = res.getStatus();
            ActiveMQRestLogger.LOGGER.debug("Status of push: " + status);
            if (status == 503) {
               String retryAfter = res.getStringHeaders().getFirst("Retry-After");
               if (retryAfter != null) {
                  wait = Long.parseLong(retryAfter) * 1000;
               }
            } else if (status == 307) {
               uri = res.getLocation().toString();
               wait = 0;
            } else if ((status >= 200 && status < 299) || status == 303 || status == 304) {
               ActiveMQRestLogger.LOGGER.debug("Success");
               return true;
            } else if (status >= 400) {
               switch (status) {
                  case 400: // these usually mean the message you are trying to send is crap, let dead letter logic take over
                  case 411:
                  case 412:
                  case 413:
                  case 414:
                  case 415:
                  case 416:
                     throw new RuntimeException("Something is wrong with the message, status returned: " + status + " for push registration of URI: " + uri);
                  case 401: // might as well consider these critical failures and abort.  Immediately signal to disable push registration depending on config
                  case 402:
                  case 403:
                  case 405:
                  case 406:
                  case 407:
                  case 417:
                  case 505:
                     return false;
                  case 404:  // request timeout, gone, and not found treat as a retry
                  case 408:
                  case 409:
                  case 410:
                     break;
                  default: // all 50x requests just retry (except 505)
                     break;
               }
            }
         } catch (Exception e) {
            ActiveMQRestLogger.LOGGER.failedToPushMessageToUri(uri, e);
            return false;
         } finally {
            if (res != null)
               res.releaseConnection();
         }
         try {
            if (wait > 0)
               Thread.sleep(wait);
         } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted");
         }
      }
      return false;
   }

   protected String createUri(ClientMessage message) {
      String uri = targetUri.build().toString();
      return uri;
   }

   static class PreemptiveAuth implements HttpRequestInterceptor {

      @Override
      public void process(final HttpRequest request, final HttpContext context) throws HttpException, IOException {
         AuthState authState = (AuthState) context.getAttribute(HttpClientContext.TARGET_AUTH_STATE);

         // If no auth scheme available yet, try to initialize it preemptively
         if (authState.getAuthScheme() == null) {
            AuthScheme authScheme = (AuthScheme) context.getAttribute("preemptive-auth");
            CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(HttpClientContext.CREDS_PROVIDER);
            HttpHost targetHost = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
            if (authScheme != null) {
               Credentials creds = credsProvider.getCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()));
               if (creds == null) {
                  throw new HttpException("No credentials for preemptive authentication");
               }
               authState.update(authScheme, creds);
            }
         }
      }
   }
}
