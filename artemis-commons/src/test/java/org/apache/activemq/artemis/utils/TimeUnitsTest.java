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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeUnitsTest {

   @Rule
   public TemporaryFolder folder = new TemporaryFolder();

   @Test
   public void testWaitOnBoolean() throws IOException {
      File tmpFile = folder.newFile("myfile.txt");
      assertTrue(tmpFile.exists());
      long begin = System.currentTimeMillis();
      boolean result = TimeUtils.waitOnBoolean(false, 100, tmpFile::exists);
      long end = System.currentTimeMillis();

      assertFalse(result);
      assertTrue(tmpFile.exists());
      //ideally the sleep time should > 2000.
      assertTrue((end - begin) >= 100);
      tmpFile.delete();
      begin = System.currentTimeMillis();
      result = TimeUtils.waitOnBoolean(false, 5000, tmpFile::exists);
      end = System.currentTimeMillis();

      assertTrue(result);
      assertFalse(tmpFile.exists());

      assertTrue((end - begin) < 5000);
   }
}
