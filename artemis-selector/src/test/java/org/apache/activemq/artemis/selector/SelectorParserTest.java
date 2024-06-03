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
package org.apache.activemq.artemis.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.activemq.artemis.selector.filter.BooleanExpression;
import org.apache.activemq.artemis.selector.filter.ComparisonExpression;
import org.apache.activemq.artemis.selector.filter.Expression;
import org.apache.activemq.artemis.selector.filter.LogicExpression;
import org.apache.activemq.artemis.selector.filter.PropertyExpression;
import org.apache.activemq.artemis.selector.filter.XPathExpression;
import org.apache.activemq.artemis.selector.impl.SelectorParser;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class SelectorParserTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public void info(String msg) {
      logger.debug(msg);
   }

   @Test
   public void testParseXPath() throws Exception {
      BooleanExpression filter = parse("XPATH '//title[@lang=''eng'']'");
      assertTrue(filter instanceof XPathExpression, "Created XPath expression");
      info("Expression: " + filter);
   }

   @Test
   public void testParseWithParensAround() throws Exception {
      String[] values = {"x = 1 and y = 2", "(x = 1) and (y = 2)", "((x = 1) and (y = 2))"};

      for (int i = 0; i < values.length; i++) {
         String value = values[i];
         info("Parsing: " + value);

         BooleanExpression andExpression = parse(value);
         assertTrue(andExpression instanceof LogicExpression, "Created LogicExpression expression");
         LogicExpression logicExpression = (LogicExpression) andExpression;
         Expression left = logicExpression.getLeft();
         Expression right = logicExpression.getRight();

         assertTrue(left instanceof ComparisonExpression, "Left is a binary filter");
         assertTrue(right instanceof ComparisonExpression, "Right is a binary filter");
         ComparisonExpression leftCompare = (ComparisonExpression) left;
         ComparisonExpression rightCompare = (ComparisonExpression) right;
         assertPropertyExpression("left", leftCompare.getLeft(), "x");
         assertPropertyExpression("right", rightCompare.getLeft(), "y");
      }
   }

   protected void assertPropertyExpression(String message, Expression expression, String expected) {
      assertTrue(expression instanceof PropertyExpression, message + ". Must be PropertyExpression");
      PropertyExpression propExp = (PropertyExpression) expression;
      assertEquals(expected, propExp.getName(), message + ". Property name");
   }

   protected BooleanExpression parse(String text) throws Exception {
      return SelectorParser.parse(text);
   }
}
