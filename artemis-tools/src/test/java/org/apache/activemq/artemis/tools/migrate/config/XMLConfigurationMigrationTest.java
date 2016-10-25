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
package org.apache.activemq.artemis.tools.migrate.config;

import javax.xml.transform.OutputKeys;
import java.io.File;
import java.util.Properties;

import org.junit.Test;

public class XMLConfigurationMigrationTest {

   @Test
   public void testQueuesReplacedWithAddresses() throws Exception {
      File brokerXml = new File(this.getClass().getClassLoader().getResource("broker.xml").toURI());
      File output = new File("target/out.xml");

      XMLConfigurationMigration tool = new XMLConfigurationMigration(brokerXml, output);

      tool.transform();

      Properties properties = new Properties();
      properties.put(OutputKeys.INDENT, "yes");
      properties.put("{http://xml.apache.org/xslt}indent-amount", "3");
      properties.put(OutputKeys.ENCODING, "UTF-8");
      tool.write(output, properties);
   }

   @Test
   public void scanAndReplaceTest() throws Exception {
      File dir = new File(this.getClass().getClassLoader().getResource("replace").getPath());
      Main.scanAndTransform(dir);
   }
}
