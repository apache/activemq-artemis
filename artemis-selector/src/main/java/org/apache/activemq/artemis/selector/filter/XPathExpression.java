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

/**
 * Used to evaluate an XPath Expression in a JMS selector.
 */
public final class XPathExpression implements BooleanExpression {

   public static XPathEvaluatorFactory XPATH_EVALUATOR_FACTORY = null;

   static {
      // Install the xalan xpath evaluator if it available.
      new XalanXPathEvaluator("//root").evaluate("<root></root>");
      try {
         XPATH_EVALUATOR_FACTORY = new XPathExpression.XPathEvaluatorFactory() {
            @Override
            public XPathExpression.XPathEvaluator create(String xpath) {
               return new XalanXPathEvaluator(xpath);
            }
         };
      } catch (Throwable e) {
      }
   }

   private final String xpath;
   private final XPathEvaluator evaluator;

   public interface XPathEvaluatorFactory {

      XPathEvaluator create(String xpath);
   }

   public interface XPathEvaluator {

      boolean evaluate(Filterable message) throws FilterException;
   }

   XPathExpression(String xpath) {
      if (XPATH_EVALUATOR_FACTORY == null) {
         throw new IllegalArgumentException("XPATH support not enabled.");
      }
      this.xpath = xpath;
      this.evaluator = XPATH_EVALUATOR_FACTORY.create(xpath);
   }

   @Override
   public Object evaluate(Filterable message) throws FilterException {
      return evaluator.evaluate(message) ? Boolean.TRUE : Boolean.FALSE;
   }

   @Override
   public String toString() {
      return "XPATH " + ConstantExpression.encodeString(xpath);
   }

   /**
    * @param message
    * @return true if the expression evaluates to Boolean.TRUE.
    * @throws FilterException
    */
   @Override
   public boolean matches(Filterable message) throws FilterException {
      Object object = evaluate(message);
      return object == Boolean.TRUE;
   }

}
