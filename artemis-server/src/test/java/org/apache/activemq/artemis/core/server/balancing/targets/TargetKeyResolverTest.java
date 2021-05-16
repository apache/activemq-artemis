/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing.targets;

import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TargetKeyResolverTest {

   @Test
   public void testClientIDKey() {
      testClientIDKey("TEST", "TEST", null);
   }

   @Test
   public void testClientIDKeyWithFilter() {
      testClientIDKey("TEST", "TEST1234", "^.{4}");
   }

   private void testClientIDKey(String expected, String clientID, String filter) {
      TargetKeyResolver targetKeyResolver = new TargetKeyResolver(TargetKey.CLIENT_ID, filter);

      Assert.assertEquals(expected, targetKeyResolver.resolve(null, clientID, null));

      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));
   }

   @Test
   public void testSNIHostKey() {
      testSNIHostKey("TEST", "TEST", null);
   }

   @Test
   public void testSNIHostKeyWithFilter() {
      testSNIHostKey("TEST", "TEST1234", "^.{4}");
   }

   private void testSNIHostKey(String expected, String sniHost, String filter) {
      Connection connection = Mockito.mock(Connection.class);

      TargetKeyResolver targetKeyResolver = new TargetKeyResolver(TargetKey.SNI_HOST, filter);

      Mockito.when(connection.getSNIHostName()).thenReturn(sniHost);
      Assert.assertEquals(expected, targetKeyResolver.resolve(connection, null, null));

      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));

      Mockito.when(connection.getSNIHostName()).thenReturn(null);
      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));
   }

   @Test
   public void testSourceIPKey() {
      testSourceIPKey("10.0.0.1", "10.0.0.1:12345", null);
   }

   @Test
   public void testSourceIPKeyWithFilter() {
      testSourceIPKey("10", "10.0.0.1:12345", "^[^.]+");
   }

   private void testSourceIPKey(String expected, String remoteAddress, String filter) {
      Connection connection = Mockito.mock(Connection.class);

      TargetKeyResolver targetKeyResolver = new TargetKeyResolver(TargetKey.SOURCE_IP, filter);

      Mockito.when(connection.getRemoteAddress()).thenReturn(remoteAddress);
      Assert.assertEquals(expected, targetKeyResolver.resolve(connection, null, null));

      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));

      Mockito.when(connection.getRemoteAddress()).thenReturn(null);
      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));
   }

   @Test
   public void testUserNameKey() {
      testUserNameKey("TEST", "TEST", null);
   }

   @Test
   public void testUserNameKeyWithFilter() {
      testUserNameKey("TEST", "TEST1234", "^.{4}");
   }

   private void testUserNameKey(String expected, String username, String filter) {
      TargetKeyResolver targetKeyResolver = new TargetKeyResolver(TargetKey.USER_NAME, filter);

      Assert.assertEquals(expected, targetKeyResolver.resolve(null, null, username));

      Assert.assertEquals(TargetKeyResolver.DEFAULT_KEY_VALUE, targetKeyResolver.resolve(null, null, null));
   }
}
