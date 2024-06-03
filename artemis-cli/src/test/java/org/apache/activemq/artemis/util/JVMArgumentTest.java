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

package org.apache.activemq.artemis.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

public class JVMArgumentTest {

   @Test
   public void testArgumentsWindows() {
      String arguments = "IF \"%JAVA_ARGS%\"==\"\" (set JAVA_ARGS= -must-go -XX:AutoBoxCacheMax=20000 -XX:+PrintClassHistogram  -XX:+UseG1GC -XX:+UseStringDeduplication -Xms333M -Xmx77G -Djava.security.auth.login.config=%ARTEMIS_ETC_DIR%\\login.config -Dhawtio.disableProxy=true -Dhawtio.offline=true -Dhawtio.realm=activemq -Dhawtio.role=amq -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=%ARTEMIS_INSTANCE_ETC_URI%\\jolokia-access.xml -Dartemis.instance=%ARTEMIS_INSTANCE%)";

      String prefix = "IF \"%JAVA_ARGS%\"==\"\" (set JAVA_ARGS= ";

      String[] fixedArguments = new String[]{"-Xmx", "-Xms"};

      HashMap<String, String> usedArgs = new HashMap<>();
      JVMArgumentParser.parseOriginalArgs(prefix, "\"", arguments, fixedArguments, usedArgs);
      assertEquals(2, usedArgs.size());
      assertEquals("-Xmx77G", usedArgs.get("-Xmx"));
      assertEquals("-Xms333M", usedArgs.get("-Xms"));

      String newLine = "IF \"%JAVA_ARGS%\"==\"\" (set JAVA_ARGS= -XX:AutoBoxCacheMax=20000 -XX:+PrintClassHistogram  -XX:+UseG1GC -XX:+UseStringDeduplication -Xms512M -Xmx1G -Djava.security.auth.login.config=%ARTEMIS_ETC_DIR%\\login.config -Dhawtio.disableProxy=true -Dhawtio.offline=true -Dhawtio.realm=activemq -Dhawtio.role=amq -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=%ARTEMIS_INSTANCE_ETC_URI%\\jolokia-access.xml -Dartemis.instance=%ARTEMIS_INSTANCE%)";

      String resultLine = JVMArgumentParser.parseNewLine(prefix, "\"", newLine, fixedArguments, usedArgs);

      System.out.println("output::" + resultLine);

      assertFalse(resultLine.contains("-must-go"));
      assertTrue(resultLine.contains("-Xmx77G"));
      assertTrue(resultLine.contains("-Xms333M"));
      assertFalse(resultLine.contains("-Xmx1G"));
      assertFalse(resultLine.contains("-Xmx512M"));
   }


   @Test
   public void testArgumentsLinux() {
      String arguments = "    JAVA_ARGS=\"-must-go -XX:AutoBoxCacheMax=20000 -XX:+PrintClassHistogram -XX:+UseG1GC -XX:+UseStringDeduplication -Xms333M -Xmx77G -Dhawtio.disableProxy=true -Dhawtio.realm=activemq -Dhawtio.offline=true -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=${ARTEMIS_INSTANCE_ETC_URI}jolokia-access.xml \"";

      String prefix = "JAVA_ARGS=";

      String[] fixedArguments = new String[]{"-Xmx", "-Xms"};

      HashMap<String, String> usedArgs = new HashMap<>();
      JVMArgumentParser.parseOriginalArgs(prefix, "\"", arguments, fixedArguments, usedArgs);
      assertEquals(2, usedArgs.size());
      assertEquals("-Xmx77G", usedArgs.get("-Xmx"));
      assertEquals("-Xms333M", usedArgs.get("-Xms"));

      String newLine = "    JAVA_ARGS= -XX:AutoBoxCacheMax=20000 -XX:+PrintClassHistogram  -XX:+UseG1GC -XX:+UseStringDeduplication -Xms512M -Xmx1G -Djava.security.auth.login.config=%ARTEMIS_ETC_DIR%\\login.config -Dhawtio.disableProxy=true -Dhawtio.offline=true -Dhawtio.realm=activemq -Dhawtio.role=amq -Dhawtio.rolePrincipalClasses=org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal -Djolokia.policyLocation=%ARTEMIS_INSTANCE_ETC_URI%\\jolokia-access.xml -Dartemis.instance=%ARTEMIS_INSTANCE%)";

      String resultLine = JVMArgumentParser.parseNewLine(prefix, "\"", newLine, fixedArguments, usedArgs);

      System.out.println("output::" + resultLine);

      assertFalse(resultLine.contains("-must-go"));
      assertTrue(resultLine.contains("-Xmx77G"));
      assertTrue(resultLine.contains("-Xms333M"));
      assertFalse(resultLine.contains("-Xmx1G"));
      assertFalse(resultLine.contains("-Xmx512M"));

      assertTrue(resultLine.startsWith("    "));
   }

}
