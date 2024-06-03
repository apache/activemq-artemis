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

import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableOutTest {

   @Test
   public void testSplitString() {
      String bigCell = "1234554321321";
      TableOut tableOut = new TableOut("|", 0, new int[] {10, 3, 3});
      ArrayList<String> lines = tableOut.splitLine(bigCell, 5);
      Assertions.assertEquals(3, lines.size());
      Assertions.assertEquals("12345", lines.get(0));
      Assertions.assertEquals("54321", lines.get(1));
      Assertions.assertEquals("321", lines.get(2));
   }

   @Test
   public void testSplitStringIdented() {
      String bigCell = "1234532132";
      TableOut tableOut = new TableOut("|", 2, new int[] {10, 3, 3});
      ArrayList<String> lines = tableOut.splitLine(bigCell, 5);
      Assertions.assertEquals(3, lines.size());
      Assertions.assertEquals("12345", lines.get(0));
      Assertions.assertEquals("  321", lines.get(1));
      Assertions.assertEquals("  32", lines.get(2));
   }

   @Test
   public void testOutLine() {
      // the output is visual, however this test is good to make sure the output at least works without any issues
      TableOut tableOut = new TableOut("|", 2, new int[] {5, 20, 20});
      tableOut.print(System.out, new String[]{"This is a big title", "1234567", "1234"});
      tableOut = new TableOut("|", 0, new int[] {10, 20, 20});
      tableOut.print(System.out, new String[]{"This is a big title", "1234567", "1234"}, new boolean[] {true, true, true});
   }

}
