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

import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Krb5LoginModuleTest {

   @Test
   public void loginFail() throws Exception {
      Krb5LoginModule underTest = new Krb5LoginModule();

      final Subject subject = new Subject();
      underTest.initialize(subject, new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         }
      }, null, null);

      assertFalse(underTest.login());
   }

   @Test
   public void loginSuccess() throws Exception {
      Krb5LoginModule underTest = new Krb5LoginModule();

      final Subject subject = new Subject();
      underTest.initialize(subject, new CallbackHandler() {
         @Override
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            ((Krb5Callback) callbacks[0]).setPeerPrincipal(new UserPrincipal("A"));
         }
      }, null, null);

      assertTrue(underTest.login());
   }

}
