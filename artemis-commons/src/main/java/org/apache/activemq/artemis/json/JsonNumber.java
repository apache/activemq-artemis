/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.activemq.artemis.json;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * JsonValue which represents a number.
 * <p>
 * The decimal point is defined as dot '.'.
 *
 * @see <a href="https://tools.ietf.org/html/rfc4627">RFC-4627 JSON Specification</a>
 */
public interface JsonNumber extends JsonValue {
   boolean isIntegral();

   int intValue();

   int intValueExact();

   long longValue();

   long longValueExact();

   BigInteger bigIntegerValue();

   BigInteger bigIntegerValueExact();

   double doubleValue();

   BigDecimal bigDecimalValue();

   default Number numberValue() {
      throw new UnsupportedOperationException();
   }

   @Override
   String toString();

   @Override
   boolean equals(Object obj);

   @Override
   int hashCode();
}