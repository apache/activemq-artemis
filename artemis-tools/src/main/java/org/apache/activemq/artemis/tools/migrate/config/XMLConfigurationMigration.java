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
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.util.Collection;
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

   // Attributes
   private static final String xPathAttrName = "@name";

   // JMS XPaths
   private static final String xPathJMS = "/configuration/jms";

   private static final String xPathJMSQueues = "/configuration/jms/queue";

   private static final String xPathJMSTopics = "/configuration/jms/topic";

   // Core Queue XPaths
   private static final String xPathQueues = "/configuration/core/queues";

   private static final String xPathQueue = "/configuration/core/queues/queue";

   private static final String xPathAddress = "address";

   private static final String xPathFilter = "filter/@string";

   private static final String xPathSelector = "selector/@string";

   private static final String xPathDurable = "durable";

   private final Map<String, Address> coreAddresses = new HashMap<>();

   private final Document document;

   private final File input;

   private final File output;

   private final Node coreElement;

   private final XPath xPath;

   public XMLConfigurationMigration(File input, File output) throws Exception {

      this.input = input;
      this.output = output;

      try {
         if (!input.exists()) {
            throw new Exception("Input file not found: " + input);
         }

         DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
         factory.setIgnoringElementContentWhitespace(true);

         DocumentBuilder db = factory.newDocumentBuilder();
         this.document = db.parse(this.input);

         xPath = XPathFactory.newInstance().newXPath();
         coreElement = (Node) xPath.evaluate("/configuration/core", document, XPathConstants.NODE);

         if (coreElement == null) {
            throw new Exception("Not a artemis config");
         }
      } catch (Exception e) {
         throw new Exception(e);
      }
   }

   public boolean transform() throws Exception {
      try {

         boolean queuesChanged = convertQueuesToAddresses();
         boolean jmsChanged = convertJMSToAddresses();

         writeAddressesToDocument();
         document.normalize();

         if (queuesChanged || jmsChanged) {
            Properties properties = new Properties();
            properties.put(OutputKeys.INDENT, "yes");
            properties.put("{http://xml.apache.org/xslt}indent-amount", "3");
            properties.put(OutputKeys.ENCODING, "UTF-8");
            write(output, properties);
            return true;
         }

      } catch (Exception e) {
         System.err.println("Error tranforming document");
         e.printStackTrace();
      }
      return false;
   }

   public boolean convertQueuesToAddresses() throws Exception {

      Node coreQueuesElement = getNode(xPathQueues);
      if (coreQueuesElement == null) {
         return false;
      }

      NodeList coreQueueElements = getNodeList(xPathQueue);
      for (int i = 0; i < coreQueueElements.getLength(); i++) {
         Node queueNode = coreQueueElements.item(i);

         Queue queue = new Queue();
         queue.setName(getString(queueNode, xPathAttrName));
         queue.setDurable(getString(queueNode, xPathDurable));
         queue.setFilter(getString(queueNode, xPathFilter));
         queue.setRoutingType("multicast");

         String addressName = getString(queueNode, xPathAddress);

         Address address;
         if (coreAddresses.containsKey(addressName)) {
            address = coreAddresses.get(addressName);
         } else {
            address = new Address();
            address.setName(addressName);
            coreAddresses.put(addressName, address);
         }
         address.getQueues().add(queue);
         address.getRoutingTypes().add("multicast");
      }

      // Remove Core Queues Element from Core
      Node queues = getNode(xPathQueues);
      if (queues != null) {
         coreElement.removeChild(queues);
      }

      return true;
   }

   public boolean convertJMSToAddresses() throws Exception {
      Node jmsElement = getNode(xPathJMS);
      if (jmsElement == null) {
         return false;
      }

      NodeList jmsQueueElements = getNodeList(xPathJMSQueues);
      for (int i = 0; i < jmsQueueElements.getLength(); i++) {
         Node jmsQueueElement = jmsQueueElements.item(i);
         String name = getString(jmsQueueElement, xPathAttrName);

         Address address;
         if (coreAddresses.containsKey(name)) {
            address = coreAddresses.get(name);
         } else {
            address = new Address();
            address.setName(name);
            address.getRoutingTypes().add("anycast");
            coreAddresses.put(name, address);
         }

         Queue queue = new Queue();
         queue.setName(name);
         queue.setDurable(getString(jmsQueueElement, xPathDurable));
         queue.setFilter(getString(jmsQueueElement, xPathSelector));
         queue.setRoutingType("anycast");
         address.getQueues().add(queue);
         address.getRoutingTypes().add("anycast");
      }

      NodeList jmsTopicElements = getNodeList(xPathJMSTopics);
      for (int i = 0; i < jmsTopicElements.getLength(); i++) {
         Node jmsTopicElement = jmsTopicElements.item(i);
         String name = getString(jmsTopicElement, xPathAttrName);

         Address address;
         if (coreAddresses.containsKey(name)) {
            address = coreAddresses.get(name);
         } else {
            address = new Address();
            address.setName(name);
            address.getRoutingTypes().add("multicast");
            coreAddresses.put(name, address);
         }
         address.getRoutingTypes().add("multicast");
      }

      jmsElement.getParentNode().removeChild(jmsElement);
      return true;
   }

   public void writeAddressesToDocument() throws XPathExpressionException {

      Element addressElement = document.createElement("addresses");
      writeAddressListToDoc(coreAddresses.values(), addressElement);
      coreElement.appendChild(addressElement);
   }

   private void writeAddressListToDoc(Collection<Address> addresses,
                                      Node addressElement) throws XPathExpressionException {
      if (addresses.isEmpty())
         return;

      for (Address addr : addresses) {
         Element eAddr = document.createElement("address");
         eAddr.setAttribute("name", addr.getName());

         Element eMulticast = null;
         if (addr.getRoutingTypes().contains("multicast")) {
            eMulticast = (Element) eAddr.getElementsByTagName("multicast").item(0);
            if (eMulticast == null) {
               eMulticast = document.createElement("multicast");
               eAddr.appendChild(eMulticast);
            }
         }

         Element eAnycast = null;
         if (addr.getRoutingTypes().contains("anycast")) {
            eAnycast = (Element) eAddr.getElementsByTagName("anycast").item(0);
            if (eAnycast == null) {
               eAnycast = document.createElement("anycast");
               eAddr.appendChild(eAnycast);
            }
         }

         if (addr.getQueues().size() > 0) {
            for (Queue queue : addr.getQueues()) {
               Element eQueue = document.createElement("queue");
               eQueue.setAttribute("name", queue.getName());

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

               if (queue.getRoutingType().equals("anycast")) {
                  eAnycast.appendChild(eQueue);
               } else if (queue.getRoutingType().equals("multicast")) {
                  eMulticast.appendChild(eQueue);
               } else {
                  System.err.print("Unknown Routing Type Found for Queue: " + queue.getName());
               }
            }
         }
         addressElement.appendChild(eAddr);
      }
   }

   public void write(File output, Properties outputProperties) throws TransformerException {
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperties(outputProperties);
      StreamResult streamResult = new StreamResult(output);
      transformer.transform(new DOMSource(document), streamResult);
   }

   private String getString(Node node, String xPathQuery) throws XPathExpressionException {
      return xPath.evaluate(xPathQuery, node, XPathConstants.STRING).toString();
   }

   private NodeList getNodeList(String xPathQuery) throws XPathExpressionException {
      return (NodeList) xPath.evaluate(xPathQuery, document, XPathConstants.NODESET);
   }

   private Node getNode(String xPathQuery) throws XPathExpressionException {
      return (Node) xPath.evaluate(xPathQuery, document, XPathConstants.NODE);
   }

}
