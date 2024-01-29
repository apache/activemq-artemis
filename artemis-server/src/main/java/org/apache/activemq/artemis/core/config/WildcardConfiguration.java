/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;

public class WildcardConfiguration implements Serializable {

   private static final long serialVersionUID = 1L;

   static final char SINGLE_WORD = '*';

   static final char ANY_WORDS = '#';

   static final char DELIMITER = '.';

   static final char ESCAPE = '\\';

   boolean routingEnabled = true;

   char singleWord = SINGLE_WORD;

   char anyWords = ANY_WORDS;

   char delimiter = DELIMITER;

   String singleWordString = String.valueOf(singleWord);

   String anyWordsString = String.valueOf(anyWords);

   String delimiterString = String.valueOf(delimiter);

   String escapeString = String.valueOf(ESCAPE);


   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof WildcardConfiguration)) {
         return false;
      }

      WildcardConfiguration that = (WildcardConfiguration) o;

      if (routingEnabled != that.routingEnabled) {
         return false;
      }
      if (singleWord != that.singleWord) {
         return false;
      }
      if (anyWords != that.anyWords) {
         return false;
      }
      if (delimiter != that.delimiter) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      int result = (routingEnabled ? 1 : 0);
      result = 31 * result + singleWord;
      result = 31 * result + anyWords;
      result = 31 * result + delimiter;
      return result;
   }

   @Override
   public String toString() {
      return "WildcardConfiguration{" +
              "routingEnabled=" + routingEnabled +
              ", anyWords=" + anyWords +
              ", singleWord=" + singleWord +
              ", delimiter=" + delimiter +
              '}';
   }

   public boolean isRoutingEnabled() {
      return routingEnabled;
   }

   public WildcardConfiguration setRoutingEnabled(boolean routingEnabled) {
      this.routingEnabled = routingEnabled;
      return this;
   }

   public char getAnyWords() {
      return anyWords;
   }

   public String getAnyWordsString() {
      return anyWordsString;
   }


   public WildcardConfiguration setAnyWords(char anyWords) {
      this.anyWords = anyWords;
      this.anyWordsString = String.valueOf(anyWords);
      return this;
   }

   public char getDelimiter() {
      return delimiter;
   }

   public String getDelimiterString() {
      return delimiterString;
   }

   public WildcardConfiguration setDelimiter(char delimiter) {
      this.delimiter = delimiter;
      this.delimiterString = String.valueOf(delimiter);
      return this;
   }

   public char getSingleWord() {
      return singleWord;
   }

   public String getSingleWordString() {
      return singleWordString;
   }

   public WildcardConfiguration setSingleWord(char singleWord) {
      this.singleWord = singleWord;
      this.singleWordString = String.valueOf(singleWord);
      return this;
   }

   /**
    * Convert the input from this WildcardConfiguration into the specified WildcardConfiguration.
    *
    * If the input already contains characters defined in the target WildcardConfiguration then those characters will
    * be escaped and preserved as such in the returned String. That said, wildcard characters which are the same
    * between the two configurations will not be escaped
    *
    * If the input already contains escaped characters defined in this WildcardConfiguration then those characters will
    * be unescaped after conversion and restored in the returned String.
    *
    * @param input  the String to convert
    * @param target the WildcardConfiguration to convert the input into
    * @return the converted String
    */
   public String convert(final String input, final WildcardConfiguration target) {
      if (this.equals(target)) {
         return input;
      } else {
         boolean escaped = isEscaped(input);
         StringBuilder result;
         if (!escaped) {
            result = new StringBuilder(target.escape(input, this));
         } else {
            result = new StringBuilder(input);
         }
         replaceChar(result, getDelimiter(), target.getDelimiter());
         replaceChar(result, getSingleWord(), target.getSingleWord());
         replaceChar(result, getAnyWords(), target.getAnyWords());
         if (escaped) {
            return unescape(result.toString());
         } else {
            return result.toString();
         }
      }
   }

   private String escape(final String input, WildcardConfiguration from) {
      String result = input.replace(escapeString, escapeString + escapeString);
      if (delimiter != from.getDelimiter()) {
         result = result.replace(getDelimiterString(), escapeString + getDelimiterString());
      }
      if (singleWord != from.getSingleWord()) {
         result = result.replace(getSingleWordString(), escapeString + getSingleWordString());
      }
      if (anyWords != from.getAnyWords()) {
         result = result.replace(getAnyWordsString(), escapeString + getAnyWordsString());
      }
      return result;
   }

   private String unescape(final String input) {
      return input
         .replace(escapeString + escapeString, escapeString)
         .replace(ESCAPE + getDelimiterString(), getDelimiterString())
         .replace(ESCAPE + getSingleWordString(), getSingleWordString())
         .replace(ESCAPE + getAnyWordsString(), getAnyWordsString());
   }

   private boolean isEscaped(final String input) {
      for (int i = 0; i < input.length() - 1; i++) {
         if (input.charAt(i) == ESCAPE && (input.charAt(i + 1) == getDelimiter() || input.charAt(i + 1) == getSingleWord() || input.charAt(i + 1) == getAnyWords())) {
            return true;
         }
      }
      return false;
   }

   /**
    * This will replace one character with another while ignoring escaped characters (i.e. those proceeded with '\').
    *
    * @param result the final result of the replacement
    * @param replace the character to replace
    * @param replacement the replacement character to use
    */
   private void replaceChar(StringBuilder result, char replace, char replacement) {
      if (replace == replacement) {
         return;
      }
      for (int i = 0; i < result.length(); i++) {
         if (result.charAt(i) == replace && (i == 0 || result.charAt(i - 1) != ESCAPE)) {
            result.setCharAt(i, replacement);
         }
      }
   }
}
