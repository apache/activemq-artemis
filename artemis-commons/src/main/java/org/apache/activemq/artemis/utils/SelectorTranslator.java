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
import java.util.List;

/**
 * This class converts a JMS selector expression into an ActiveMQ Artemis core filter expression.
 *
 * JMS selector and ActiveMQ Artemis filters use the same syntax but have different identifiers.
 *
 * We basically just need to replace the JMS header and property Identifier names
 * with the corresponding ActiveMQ Artemis field and header Identifier names.
 *
 * We must be careful not to substitute any literals, or identifiers whose name contains the name
 * of one we want to substitute.
 *
 * This makes it less trivial than a simple search and replace.
 */
public class SelectorTranslator {

   public static String convertToActiveMQFilterString(final String selectorString) {
      if (selectorString == null) {
         return null;
      }

      // First convert any JMS header identifiers

      String filterString = SelectorTranslator.parse(selectorString, "JMSDeliveryMode", "AMQDurable");
      filterString = SelectorTranslator.parse(filterString, "'PERSISTENT'", "'DURABLE'");
      filterString = SelectorTranslator.parse(filterString, "'NON_PERSISTENT'", "'NON_DURABLE'");
      filterString = SelectorTranslator.parse(filterString, "JMSPriority", "AMQPriority");
      filterString = SelectorTranslator.parse(filterString, "JMSTimestamp", "AMQTimestamp");
      filterString = SelectorTranslator.parse(filterString, "JMSMessageID", "AMQUserID");
      filterString = SelectorTranslator.parse(filterString, "JMSExpiration", "AMQExpiration");
      filterString = SelectorTranslator.parse(filterString, "JMSXGroupID", "AMQGroupID");

      return filterString;

   }

   public static String convertHQToActiveMQFilterString(final String hqFilterString) {
      if (hqFilterString == null) {
         return null;
      }

      String filterString = SelectorTranslator.parse(hqFilterString, "HQDurable", "AMQDurable");
      filterString = SelectorTranslator.parse(filterString, "HQPriority", "AMQPriority");
      filterString = SelectorTranslator.parse(filterString, "HQTimestamp", "AMQTimestamp");
      filterString = SelectorTranslator.parse(filterString, "HQUserID", "AMQUserID");
      filterString = SelectorTranslator.parse(filterString, "HQExpiration", "AMQExpiration");

      return filterString;

   }

   private static String parse(final String input, final String match, final String replace) {
      final char quote = '\'';

      boolean inQuote = false;

      int matchPos = 0;

      List<Integer> positions = new ArrayList<>();

      boolean replaceInQuotes = match.charAt(0) == quote;

      for (int i = 0; i < input.length(); i++) {
         char c = input.charAt(i);

         if (c == quote) {
            inQuote = !inQuote;
         }

         if ((!inQuote || replaceInQuotes) && c == match.charAt(matchPos)) {
            matchPos++;

            if (matchPos == match.length()) {

               boolean matched = true;

               // Check that name is not part of another identifier name

               // Check character after match
               if (i < input.length() - 1 && Character.isJavaIdentifierPart(input.charAt(i + 1))) {
                  matched = false;
               }

               // Check character before match
               int posBeforeStart = i - match.length();

               if (posBeforeStart >= 0 && Character.isJavaIdentifierPart(input.charAt(posBeforeStart))) {
                  matched = false;
               }

               if (matched) {
                  positions.add(i - match.length() + 1);
               }

               // check previous character too

               matchPos = 0;
            }
         } else {
            matchPos = 0;
         }
      }

      if (!positions.isEmpty()) {
         StringBuffer buff = new StringBuffer();

         int startPos = 0;

         for (int pos : positions) {
            String substr = input.substring(startPos, pos);

            buff.append(substr);

            buff.append(replace);

            startPos = pos + match.length();
         }

         if (startPos < input.length()) {
            buff.append(input.substring(startPos, input.length()));
         }

         return buff.toString();
      } else {
         return input;
      }
   }
}
