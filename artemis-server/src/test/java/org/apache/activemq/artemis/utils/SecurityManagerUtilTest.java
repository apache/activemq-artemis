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

package org.apache.activemq.artemis.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.security.auth.Subject;
import java.security.Principal;

import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.junit.jupiter.api.Test;

public class SecurityManagerUtilTest {

   @Test
   public void getUserFromSubject() throws Exception {
      assertNull(SecurityManagerUtil.getUserFromSubject(null, null));
      Subject subject = new Subject();
      assertNull(SecurityManagerUtil.getUserFromSubject(subject, UserPrincipal.class));
      subject.getPrincipals().add(new RolePrincipal("r"));
      assertNull(SecurityManagerUtil.getUserFromSubject(subject, UserPrincipal.class));
      subject.getPrincipals().add(new UserPrincipal("u"));
      assertEquals("u", SecurityManagerUtil.getUserFromSubject(subject, UserPrincipal.class));
   }

   class UserFromOtherDomainPrincipal implements Principal {
      @Override
      public String getName() {
         return "P";
      }
   }

   @Test
   public void getUserFromForeignPrincipalInSubject() throws Exception {
      Subject subject = new Subject();
      subject.getPrincipals().add(new UserFromOtherDomainPrincipal());
      assertNull(SecurityManagerUtil.getUserFromSubject(subject, UserPrincipal.class));
   }
}