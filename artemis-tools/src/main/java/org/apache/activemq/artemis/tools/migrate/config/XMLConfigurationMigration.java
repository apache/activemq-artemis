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
package org.apache.activemq.artemis.tools.migrate.config;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.artemis.tools.migrate.config.addressing.Address;
import org.apache.activemq.artemis.tools.migrate.config.addressing.Queue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLConfigurationMigration {

   private static XMLConfigurationMigration migration;

   private final Document document;

   public static void main(String[] args) throws Exception {

      if (args.length == 0) {
         System.err.println("Invalid args");
         printUsage();
      } else {
         File input = new File(args[0]);
         if (input.isDirectory()) {
            System.out.println("Scanning directory: " + input.getAbsolutePath());
            recursiveTransform(input);
         } else {
            if (args.length != 2) {
               System.err.println("Invalid args");
               printUsage();
            } else {
               transform(input, new File(args[1]));
            }
         }
      }
   }

   private static void recursiveTransform(File root) throws Exception {
      for (File file : root.listFiles()) {
         scanAndTransform(file);
      }
   }

   public static void scanAndTransform(File pFile) throws Exception {
      try {
         for (File f : pFile.listFiles()) {
            if (f.isDirectory()) {
               scanAndTransform(f);
            } else {
               try {
                  if (f.getName().endsWith("xml")) {
                     File file = new File(f.getAbsolutePath() + ".new");
                     if (transform(f, file)) {
                        File r = new File(f.getAbsolutePath());

                        f.renameTo(new File(f.getAbsolutePath() + ".bk"));
                        file.renameTo(r);
                     }
                  }
               } catch (Exception e) {
                  //continue
               }
            }
         }
      } catch (NullPointerException e) {
         System.out.println(pFile.getAbsoluteFile());
      }
   }

   public static void printUsage() {
      System.out.println("Please specify a directory to scan, or input and output file");
   }

   public static boolean transform(File input, File output) throws Exception {

      migration = new XMLConfigurationMigration(input);
      try {
         if (!input.exists()) {
            System.err.println("Input file not found: " + input);
         }

         if (migration.convertQueuesToAddresses()) {
            Properties properties = new Properties();
            properties.put(OutputKeys.INDENT, "yes");
            properties.put("{http://xml.apache.org/xslt}indent-amount", "3");
            properties.put(OutputKeys.ENCODING, "UTF-8");
            migration.write(output, properties);
            return true;
         }
      } catch (Exception e) {
         System.err.println("Error tranforming document");
         e.printStackTrace();
      }
      return false;
   }

   public XMLConfigurationMigration(File input) throws Exception {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringElementContentWhitespace(true);

      DocumentBuilder db = factory.newDocumentBuilder();
      this.document = db.parse(input);
   }

   public boolean convertQueuesToAddresses() throws Exception {

      Map<String, Address> addresses = new HashMap<>();

      String xPathQueues = "/configuration/core/queues";
      String xPathQueue = "/configuration/core/queues/queue";
      String xPathAttrName = "@name";
      String xPathAddress = "address";
      String xPathFilter = "filter/@string";
      String xPathDurable = "durable";

      XPath xPath = XPathFactory.newInstance().newXPath();

      NodeList xpathResult = (NodeList) xPath.evaluate(xPathQueue, document, XPathConstants.NODESET);
      if (xpathResult == null || xpathResult.getLength() == 0) {
         // doesn't require change
         return false;
      }

      for (int i = 0; i < xpathResult.getLength(); i++) {
         Node queueNode = xpathResult.item(i);

         Queue queue = new Queue();
         queue.setName(xPath.evaluate(xPathAttrName, queueNode, XPathConstants.STRING).toString());
         queue.setDurable(xPath.evaluate(xPathDurable, queueNode, XPathConstants.STRING).toString());
         queue.setFilter(xPath.evaluate(xPathFilter, queueNode, XPathConstants.STRING).toString());

         String addressName = xPath.evaluate(xPathAddress, queueNode, XPathConstants.STRING).toString();
         Address address;

         if (addresses.containsKey(addressName)) {
            address = addresses.get(addressName);
         } else {
            address = new Address();
            address.setName(addressName);
            addresses.put(addressName, address);
         }
         address.getQueues().add(queue);
      }

      Node queues = ((Node) xPath.evaluate(xPathQueues, document, XPathConstants.NODE));

      if (queues != null) {
         Node core = queues.getParentNode();
         core.removeChild(queues);

         Element a = document.createElement("addresses");
         for (Address addr : addresses.values()) {
            Element eAddr = document.createElement("address");
            eAddr.setAttribute("name", addr.getName());
            eAddr.setAttribute("type", addr.getRoutingType());

            if (addr.getQueues().size() > 0) {
               Element eQueues = document.createElement("queues");
               for (Queue queue : addr.getQueues()) {
                  Element eQueue = document.createElement("queue");
                  eQueue.setAttribute("name", queue.getName());
                  eQueue.setAttribute("max-consumers", addr.getDefaultMaxConsumers());
                  eQueue.setAttribute("delete-on-no-consumers", addr.getDefaultDeleteOnNoConsumers());

                  if (queue.getDurable() != null && !queue.getDurable().isEmpty()) {
                     Element eDurable = document.createElement("durable");
                     eDurable.setTextContent(queue.getDurable());
                     eQueue.appendChild(eDurable);
                  }

                  if (queue.getFilter() != null && !queue.getFilter().isEmpty()) {
                     Element eFilter = document.createElement("filter");
                     eFilter.setAttribute("string", queue.getFilter());
                     eQueue.appendChild(eFilter);
                  }

                  eQueues.appendChild(eQueue);
               }
               eAddr.appendChild(eQueues);
            }
            a.appendChild(eAddr);
         }
         core.appendChild(a);
      }

      document.normalize();
      return true;
   }

   public void write(File output, Properties outputProperties) throws TransformerException {
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperties(outputProperties);
      StreamResult streamResult = new StreamResult(output);
      transformer.transform(new DOMSource(document), streamResult);
   }
}
