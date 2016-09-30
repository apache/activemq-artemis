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
package org.apache.activemq.artemis.dto;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlUtil {

   /**
    * Changes ${property} with values from a properties object
    */
   static class PropertiesFilter extends StreamReaderDelegate {

      static final Pattern pattern = Pattern.compile("\\$\\{([^\\}]+)\\}");
      private final Properties props;

      PropertiesFilter(XMLStreamReader parent, Properties props) {
         super(parent);
         this.props = props;
      }

      @Override
      public String getAttributeValue(int index) {
         return filter(super.getAttributeValue(index));
      }

      public String filter(String str) {
         int start = 0;
         while (true) {
            Matcher matcher = pattern.matcher(str);
            if (!matcher.find(start)) {
               break;
            }
            String group = matcher.group(1);
            String property = props.getProperty(group);
            if (property != null) {
               str = matcher.replaceFirst(Matcher.quoteReplacement(property));
            } else {
               start = matcher.end();
            }
         }
         return str;
      }

   }

   private static final XMLInputFactory factory = XMLInputFactory.newInstance();

   public static <T> T decode(Class<T> clazz, File configuration) throws Exception {
      return decode(clazz, configuration, null, null);
   }

   /**
    * We offer parameters for artemisInstance and artemisHome as they could be coming from the CLI or Maven Plugin
    */
   public static <T> T decode(Class<T> clazz,
                              File configuration,
                              String artemisHome,
                              String artemisInstance) throws Exception {
      JAXBContext jaxbContext = JAXBContext.newInstance("org.apache.activemq.artemis.dto");

      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      sf.setFeature("http://apache.org/xml/features/validation/schema-full-checking", false);
      InputStream xsdStream = XmlUtil.class.getClassLoader().getResourceAsStream("org.apache.activemq/dto/activemq.xsd");
      StreamSource xsdSource = new StreamSource(xsdStream);
      Schema schema = sf.newSchema(xsdSource);
      unmarshaller.setSchema(schema);

      Properties props = new Properties(System.getProperties());
      if (artemisHome != null) {
         props.put("artemis.home", artemisHome);
      }

      if (artemisInstance != null) {
         props.put("artemis.instance", artemisInstance);
      }

      XMLStreamReader reader = factory.createXMLStreamReader(new FileInputStream(configuration));

      reader = new PropertiesFilter(reader, props);

      return clazz.cast(unmarshaller.unmarshal(reader));
   }

}
