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
package org.apache.activemq.artemis.component;

import org.apache.activemq.artemis.logs.AuditLogger;
import org.eclipse.jetty.server.Request;

import javax.security.auth.Subject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

/*
* This intercepts all calls made via jolokia
* */
public class JolokiaFilter implements Filter {
   @Override
   public void init(FilterConfig filterConfig) {
   }

   @Override
   public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      /*
      * this is the only place we can catch the remote address of the calling console client thru Jolokia.
      * We set the address on the calling thread which will end up in JMX audit logging
      * */
      if (AuditLogger.isAnyLoggingEnabled() && servletRequest != null) {
         String remoteHost = servletRequest.getRemoteHost();
         AuditLogger.setRemoteAddress(remoteHost + ":" + servletRequest.getRemotePort());
      }
      filterChain.doFilter(servletRequest, servletResponse);
      /*
      * This is the only place we can get access to the authenticated subject on invocations after the login has happened.
      * we set the subject for audit logging
      * */
      if (AuditLogger.isAnyLoggingEnabled()) {
         try {
            HttpSession session = ((Request) servletRequest).getSession();
            Subject subject = (Subject) session.getAttribute("subject");
            AuditLogger.setCurrentCaller(subject);
         } catch (Throwable e) {
            //best effort
         }
      }
   }

   @Override
   public void destroy() {
   }
}
