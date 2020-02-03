/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

   boolean routingEnabled = true;

   char singleWord = SINGLE_WORD;

   char anyWords = ANY_WORDS;

   char delimiter = DELIMITER;

   String singleWordString = String.valueOf(singleWord);

   String anyWordsString = String.valueOf(anyWords);

   String delimiterString = String.valueOf(delimiter);


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof WildcardConfiguration)) return false;

      WildcardConfiguration that = (WildcardConfiguration) o;

      if (routingEnabled != that.routingEnabled) return false;
      if (singleWord != that.singleWord) return false;
      if (anyWords != that.anyWords) return false;
      return delimiter == that.delimiter;

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

   public void setRoutingEnabled(boolean routingEnabled) {
      this.routingEnabled = routingEnabled;
   }

   public char getAnyWords() {
      return anyWords;
   }

   public String getAnyWordsString() {
      return anyWordsString;
   }


   public void setAnyWords(char anyWords) {
      this.anyWords = anyWords;
      this.anyWordsString = String.valueOf(anyWords);
   }

   public char getDelimiter() {
      return delimiter;
   }

   public String getDelimiterString() {
      return delimiterString;
   }

   public void setDelimiter(char delimiter) {
      this.delimiter = delimiter;
      this.delimiterString = String.valueOf(delimiter);
   }

   public char getSingleWord() {
      return singleWord;
   }

   public String getSingleWordString() {
      return singleWordString;
   }

   public void setSingleWord(char singleWord) {
      this.singleWord = singleWord;
      this.singleWordString = String.valueOf(singleWord);
   }

   public String convert(String filter, WildcardConfiguration to) {
      return filter.replace(getDelimiter(), to.getDelimiter())
              .replace(getSingleWord(), to.getSingleWord())
              .replace(getAnyWords(), to.getAnyWords());
   }

}
