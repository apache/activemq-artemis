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
package org.apache.activemq.cli.test;

import org.apache.activemq.artemis.cli.commands.util.HashUtil;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HashUtilTest {

   @Test
   public void testDefaultHashFormat() throws Exception {
      final String password = "helloworld";
      String hash = HashUtil.tryHash(new TestActionContext(), password);
      String hashStr = PasswordMaskingUtil.unwrap(hash);
      System.out.println("hashString: " + hashStr);
      String[] parts = hashStr.split(":");
      assertEquals(3, parts.length);
      //first part should be able to convert to an int
      Integer.parseInt(parts[0]);
      //second and third parts are all hex values
      checkHexBytes(parts[1], parts[2]);
   }

   private void checkHexBytes(String... parts) throws Exception {
      for (String p : parts) {
         assertTrue(p.matches("^[0-9A-F]+$"));
      }
   }
}
