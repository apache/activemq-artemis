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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.StringReader;

import org.apache.xpath.CachedXPathAPI;
import org.apache.xpath.objects.XObject;
import org.w3c.dom.Document;
import org.w3c.dom.traversal.NodeIterator;
import org.xml.sax.InputSource;

public class XalanXPathEvaluator implements XPathExpression.XPathEvaluator {

   private final String xpath;

   public XalanXPathEvaluator(String xpath) {
      this.xpath = xpath;
   }

   @Override
   public boolean evaluate(Filterable m) throws FilterException {
      String stringBody = m.getBodyAs(String.class);
      if (stringBody != null) {
         return evaluate(stringBody);
      }
      return false;
   }

   protected boolean evaluate(String text) {
      return evaluate(new InputSource(new StringReader(text)));
   }

   protected boolean evaluate(InputSource inputSource) {
      try {
         DocumentBuilder dbuilder = createDocumentBuilder();
         Document doc = dbuilder.parse(inputSource);

         //An XPath expression could return a true or false value instead of a node.
         //eval() is a better way to determine the boolean value of the exp.
         //For compliance with legacy behavior where selecting an empty node returns true,
         //selectNodeIterator is attempted in case of a failure.

         CachedXPathAPI cachedXPathAPI = new CachedXPathAPI();
         XObject result = cachedXPathAPI.eval(doc, xpath);
         if (result.bool())
            return true;
         else {
            NodeIterator iterator = cachedXPathAPI.selectNodeIterator(doc, xpath);
            return (iterator.nextNode() != null);
         }
      } catch (Throwable e) {
         return false;
      }
   }

   private DocumentBuilder createDocumentBuilder() throws ParserConfigurationException {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);

      factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

      return factory.newDocumentBuilder();
   }
}
