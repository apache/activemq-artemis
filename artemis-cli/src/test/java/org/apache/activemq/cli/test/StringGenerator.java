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

import java.util.Random;

/**
 * Generate a random string.
 */
class StringGenerator {
   private String letters = "abcdefghijklmnopqrstuvwxyz";

   private String digits = "0123456789";

   private String symbols = "~!@#$%^&*()_+{}|?><,./";

   private String nonLatinLetters = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя";

   String generateRandomString(int length) {
      String initialString = letters + letters.toUpperCase() + nonLatinLetters + nonLatinLetters.toUpperCase()
              + symbols + digits;

      StringBuilder result = new StringBuilder();
      Random random = new Random();

      for (int i = 0; i < length; i++) {
         result.append(initialString.charAt(random.nextInt(initialString.length())));
      }
      return result.toString();
   }

}
