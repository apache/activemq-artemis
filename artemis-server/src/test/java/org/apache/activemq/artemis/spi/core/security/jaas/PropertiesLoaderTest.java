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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PropertiesLoaderTest {

   @Test
   void load(@TempDir Path tempDir) throws IOException {
      Path file = tempDir.resolve("p.properties");
      Properties properties = new Properties();
      properties.put("p1", "b");
      properties.put("p2", "b");
      properties.put("p3", "/b/"); // regexp

      FileWriter fileWriter = new FileWriter(file.toFile());
      properties.store(fileWriter, "");

      PropertiesLoader underTest = new PropertiesLoader();
      HashMap options = new HashMap();
      options.put("baseDir", file.getParent().toString());
      ReloadableProperties props = underTest.load("", file.toFile().getName(), options, (String v) -> v.toUpperCase(Locale.ROOT));

      assertTrue(props.getProps().getProperty("p1").equals("B"));
      assertTrue(props.getProps().getProperty("p2").equals("B"));
      assertFalse(props.getProps().getProperty("p3").contains("B"));
   }
}