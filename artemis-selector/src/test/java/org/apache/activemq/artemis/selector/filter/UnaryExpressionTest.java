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

import java.util.Collections;

import org.apache.activemq.artemis.selector.impl.SelectorParser;
import org.junit.Assert;
import org.junit.Test;

public class UnaryExpressionTest {

   @Test
   public void testEquals() throws Exception {
      BooleanExpression expr1 = UnaryExpression.createNOT(SelectorParser.parse("x = 1"));
      BooleanExpression expr2 = UnaryExpression.createNOT(SelectorParser.parse("x = 1"));
      Assert.assertTrue("Created unary expression 1", expr1 instanceof UnaryExpression);
      Assert.assertTrue("Created unary expression 2", expr2 instanceof UnaryExpression);
      Assert.assertEquals("Unary expressions are equal", expr1, expr2);
   }

   @Test
   public void testInExpressionToString() throws Exception {
      BooleanExpression expr;
      expr = UnaryExpression.createInExpression(new PropertyExpression("foo"), Collections.<Object>singletonList("bar"), false);
      Assert.assertTrue(expr.toString().matches("foo\\s+IN\\s+.*bar.*"));
      expr = UnaryExpression.createInExpression(new PropertyExpression("foo"), Collections.emptyList(), false);
      Assert.assertTrue(expr.toString().matches("foo\\s+IN\\s+.*"));
   }
}
