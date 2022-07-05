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
package org.apache.activemq.artemis.jms.example;

import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.junit.ClassRule;
import org.junit.Test;

// Boiler plate JUnit test class, leveraging integrations to stand up test LDAP server and then run example during test.
@CreateDS(name = "myDS", partitions = {@CreatePartition(name = "test", suffix = "dc=activemq,dc=org")})
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)})
@ApplyLdifFiles({"example.ldif"})
public class SecurityExampleTestLdapServer {

   @ClassRule
   public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

   @Test
   public void securityExampleWrapperTest() throws Exception {
      System.out.println("-------------------------------------------------------------------------------------");
      System.out.println("======== Running Example Application Code ========");

      SecurityExample.securityExample();

      System.out.println("======== Complete, cleaning up ========");
   }
}
