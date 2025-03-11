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
package org.apache.activemq.artemis.dto;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class XmlUtilTest {

   @Test
   public void testPropertySubstituion(@TempDir Path tempDir) throws Exception {
      final String SYSTEM_PROP_NAME = "sysPropName";
      final String SYSTEM_PROP_VALUE = "sysPropValue";
      System.setProperty(SYSTEM_PROP_NAME, SYSTEM_PROP_VALUE);

      // since System.getenv() returns an immutable Map we rely here on an environment variable that is likely to exist
      final String ENV_VAR_NAME = "HOME";

      String data = """
         <broker xmlns="http://activemq.apache.org/schema">
            <jaas-security domain="${%s}"/>
            <server configuration="${%s}"/>
         </broker>
         """.formatted(SYSTEM_PROP_NAME, ENV_VAR_NAME);

      Path tempFile = Files.createTempFile(tempDir, "testBootstrap", ".xml");
      Files.write(tempFile, data.getBytes(), StandardOpenOption.WRITE);
      assertTrue(Files.exists(tempFile));

      BrokerDTO brokerDTO = XmlUtil.decode(BrokerDTO.class, tempFile.toFile());
      assertEquals(SYSTEM_PROP_VALUE, ((JaasSecurityDTO)brokerDTO.security).domain);
      assertEquals(System.getenv(ENV_VAR_NAME), brokerDTO.server.configuration);
   }
}
