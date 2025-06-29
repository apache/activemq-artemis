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
package org.apache.activemq.artemis.selector.filter;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Represents a constant expression
 */
public class ConstantExpression implements Expression {

   static class BooleanConstantExpression extends ConstantExpression implements BooleanExpression {

      BooleanConstantExpression(Object value) {
         super(value);
      }

      @Override
      public boolean matches(Filterable message) throws FilterException {
         Object object = evaluate(message);
         return object != null && object == Boolean.TRUE;
      }
   }

   public static final BooleanConstantExpression NULL = new BooleanConstantExpression(null);
   public static final BooleanConstantExpression TRUE = new BooleanConstantExpression(Boolean.TRUE);
   public static final BooleanConstantExpression FALSE = new BooleanConstantExpression(Boolean.FALSE);

   private Object value;

   public ConstantExpression(Object value) {
      this.value = value;
   }

   public static ConstantExpression createFromDecimal(String text) {

      // Strip off the 'l' or 'L' if needed.
      if (text.endsWith("l") || text.endsWith("L")) {
         text = text.substring(0, text.length() - 1);
      }

      Number value;
      try {
         value = Long.valueOf(text);
      } catch (NumberFormatException e) {
         // The number may be too big to fit in a long.
         value = new BigDecimal(text);
      }

      long l = value.longValue();
      if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
         value = value.intValue();
      }
      return new ConstantExpression(value);
   }

   public static ConstantExpression createFromHex(String text) {
      Number value = Long.parseLong(text.substring(2), 16);
      long l = value.longValue();
      if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
         value = value.intValue();
      }
      return new ConstantExpression(value);
   }

   public static ConstantExpression createFromOctal(String text) {
      Number value = Long.parseLong(text, 8);
      long l = value.longValue();
      if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
         value = value.intValue();
      }
      return new ConstantExpression(value);
   }

   public static ConstantExpression createFloat(String text) {
      Number value = Double.valueOf(text);
      return new ConstantExpression(value);
   }

   @Override
   public Object evaluate(Filterable message) throws FilterException {
      return value;
   }

   public Object getValue() {
      return value;
   }

   @Override
   public String toString() {
      if (value == null) {
         return "NULL";
      }
      if (value instanceof Boolean booleanValue) {
         return booleanValue ? "TRUE" : "FALSE";
      }
      if (value instanceof String string) {
         return encodeString(string);
      }
      return value.toString();
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }

   @Override
   public boolean equals(final Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof ConstantExpression other)) {
         return false;
      }

      return Objects.equals(value, other.value);
   }

   /**
    * Encodes the value of string so that it looks like it would look like when it was provided in a selector.
    */
   public static String encodeString(String s) {
      StringBuilder b = new StringBuilder();
      b.append('\'');
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         if (c == '\'') {
            b.append(c);
         }
         b.append(c);
      }
      b.append('\'');
      return b.toString();
   }

}
