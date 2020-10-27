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

import org.apache.activemq.artemis.core.config.impl.Validators;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLConfigurationUtil {

   public static final String getAttributeValue(Node element, String attribute) {
      Node node = element.getAttributes().getNamedItem(attribute);
      if (node == null) {
         return null;
      } else {
         return node.getNodeValue();
      }

   }

   public static final String getTrimmedTextContent(Node element) {
      String content = element.getTextContent();
      if (content == null)
         return null;
      return content.trim();
   }

   public static final Double getDouble(final Element e,
                                        final String name,
                                        final double def,
                                        final Validators.Validator validator) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         double val = XMLUtil.parseDouble(nl.item(0));
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final String getString(final Element e,
                                        final String name,
                                        final String def,
                                        final Validators.Validator validator) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         String val = nl.item(0).getTextContent().trim();
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final Long getLong(final Element e,
                                    final String name,
                                    final long def,
                                    final Validators.Validator validator) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         long val = XMLUtil.parseLong(nl.item(0));
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final Long getTextBytesAsLongBytes(final Element e,
                                    final String name,
                                    final long def,
                                    final Validators.Validator validator) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         long val = ByteUtil.convertTextBytes(nl.item(0).getTextContent().trim());
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final Integer getAttributeInteger(final Element e,
                                          final String name,
                                          final Integer def,
                                          final Validators.Validator validator) {
      String attribute = e.getAttribute(name);
      if (attribute != null && !attribute.equals("")) {
         int val = XMLUtil.parseInt(e, attribute);
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final Integer getInteger(final Element e,
                                          final String name,
                                          final Integer def,
                                          final Validators.Validator validator) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         int val = XMLUtil.parseInt(nl.item(0));
         validator.validate(name, val);
         return val;
      } else {
         return def;
      }
   }

   public static final Integer getTextBytesAsIntBytes(final Element e,
                                          final String name,
                                          final int def,
                                          final Validators.Validator validator) {
      return getTextBytesAsLongBytes(e, name, def, validator).intValue();
   }

   public static final Boolean getBoolean(final Element e, final String name, final Boolean def) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         return XMLUtil.parseBoolean(nl.item(0));
      } else {
         return def;
      }
   }

   public static final Boolean getBooleanAttribute(final Element e, final String name, final Boolean def) {
      String attributeValue = e.getAttribute(name);
      if (attributeValue == null || attributeValue.isEmpty()) {
         return def;
      } else {
         return Boolean.parseBoolean(attributeValue);
      }
   }


   public static final Boolean parameterExists(final Element e, final String name) {
      NodeList nl = e.getElementsByTagName(name);
      if (nl.getLength() > 0) {
         return true;
      } else {
         return false;
      }
   }
}
