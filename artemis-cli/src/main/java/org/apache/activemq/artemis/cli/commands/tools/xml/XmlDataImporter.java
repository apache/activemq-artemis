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
package org.apache.activemq.artemis.cli.commands.tools.xml;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientRequestor;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.cli.commands.ActionAbstract;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.ListUtil;
import org.jboss.logging.Logger;

/**
 * Read XML output from <code>org.apache.activemq.artemis.core.persistence.impl.journal.XmlDataExporter</code>, create a core session, and
 * send the messages to a running instance of ActiveMQ Artemis.  It uses the StAX <code>javax.xml.stream.XMLStreamReader</code>
 * for speed and simplicity.
 */
@Command(name = "imp", description = "Import all message-data using an XML that could be interpreted by any system.")
public final class XmlDataImporter extends ActionAbstract {

   private static final Logger logger = Logger.getLogger(XmlDataImporter.class);

   private XMLStreamReader reader;

   private XMLMessageImporter messageReader;

   // this session is really only needed if the "session" variable does not auto-commit sends
   ClientSession managementSession;

   boolean localSession = false;

   final Map<String, String> addressMap = new HashMap<>();

   final Map<String, Long> queueIDs = new HashMap<>();

   HashMap<String, String> oldPrefixTranslation = new HashMap<>();

   private ClientSession session;

   @Option(name = "--host", description = "The host used to import the data (default localhost)")
   public String host = "localhost";

   @Option(name = "--port", description = "The port used to import the data (default 61616)")
   public int port = 61616;

   @Option(name = "--transaction", description = "If this is set to true you will need a whole transaction to commit at the end. (default false)")
   public boolean transactional;

   @Option(name = "--user", description = "User name used to import the data. (default null)")
   public String user = null;

   @Option(name = "--password", description = "User name used to import the data. (default null)")
   public String password = null;

   @Option(name = "--input", description = "The input file name (default=exp.dmp)", required = true)
   public String input = "exp.dmp";

   @Option(name = "--sort", description = "Sort the messages from the input (used for older versions that won't sort messages)")
   public boolean sort = false;

   @Option(name = "--legacy-prefixes", description = "Do not remove prefixes from legacy imports")
   public boolean legacyPrefixes = false;

   TreeSet<XMLMessageImporter.MessageInfo> messages;

   public String getPassword() {
      return password;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public String getUser() {
      return user;
   }

   public void setUser(String user) {
      this.user = user;
   }

   @Override
   public Object execute(ActionContext context) throws Exception {
      process(input, host, port, transactional);
      return null;
   }

   public void process(String inputFile, String host, int port, boolean transactional) throws Exception {
      this.process(new FileInputStream(inputFile), host, port, transactional);
   }

   /**
    * This is the normal constructor for programmatic access to the
    * <code>org.apache.activemq.artemis.core.persistence.impl.journal.XmlDataImporter</code> if the session passed
    * in uses auto-commit for sends.
    * <br>
    * If the session needs to be transactional then use the constructor which takes 2 sessions.
    *
    * @param inputStream the stream from which to read the XML for import
    * @param session     used for sending messages, must use auto-commit for sends
    */
   public void process(InputStream inputStream, ClientSession session) throws Exception {
      this.process(inputStream, session, null);
   }

   /**
    * This is the constructor to use if you wish to import all messages transactionally.
    * <br>
    * Pass in a session which doesn't use auto-commit for sends, and one that does (for management
    * operations necessary during import).
    *
    * @param inputStream       the stream from which to read the XML for import
    * @param session           used for sending messages, doesn't need to auto-commit sends
    * @param managementSession used for management queries, must use auto-commit for sends
    */
   public void process(InputStream inputStream,
                       ClientSession session,
                       ClientSession managementSession) throws Exception {
      reader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
      messageReader = new XMLMessageImporter(reader, session);
      messageReader.setOldPrefixTranslation(oldPrefixTranslation);

      this.session = session;
      if (managementSession != null) {
         this.managementSession = managementSession;
      } else {
         this.managementSession = session;
      }

      processXml();

   }

   public void process(InputStream inputStream, String host, int port, boolean transactional) throws Exception {
      HashMap<String, Object> connectionParams = new HashMap<>();
      connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
      connectionParams.put(TransportConstants.PORT_PROP_NAME, Integer.toString(port));
      ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams));
      ClientSessionFactory sf = serverLocator.createSessionFactory();

      ClientSession session;
      ClientSession managementSession;

      if (user != null || password != null) {
         session = sf.createSession(user, password, false, !transactional, true, false, 0);
         managementSession = sf.createSession(user, password, false, true, true, false, 0);
      } else {
         session = sf.createSession(false, !transactional, true);
         managementSession = sf.createSession(false, true, true);
      }
      localSession = true;

      process(inputStream, session, managementSession);
   }

   public void validate(String file) throws Exception {
      validate(new FileInputStream(file));
   }

   public void validate(InputStream inputStream) throws Exception {
      XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
      SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema schema = factory.newSchema(XmlDataImporter.findResource("schema/artemis-import-export.xsd"));

      Validator validator = schema.newValidator();
      validator.validate(new StAXSource(reader));
      reader.close();
   }

   private static URL findResource(final String resourceName) {
      return AccessController.doPrivileged(new PrivilegedAction<URL>() {
         @Override
         public URL run() {
            return ClassloadingUtil.findResource(resourceName);
         }
      });
   }

   private void processXml() throws Exception {
      if (sort) {
         messages = new TreeSet<XMLMessageImporter.MessageInfo>(new Comparator<XMLMessageImporter.MessageInfo>() {
            @Override
            public int compare(XMLMessageImporter.MessageInfo o1, XMLMessageImporter.MessageInfo o2) {
               if (o1.id == o2.id) {
                  return 0;
               } else if (o1.id > o2.id) {
                  return 1;
               } else {
                  return -1;
               }
            }
         });
      }
      try {
         while (reader.hasNext()) {
            if (logger.isDebugEnabled()) {
               logger.debug("EVENT:[" + reader.getLocation().getLineNumber() + "][" + reader.getLocation().getColumnNumber() + "] ");
            }
            if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
               if (XmlDataConstants.OLD_BINDING.equals(reader.getLocalName())) {
                  oldBinding(); // export from 1.x
               } else if (XmlDataConstants.QUEUE_BINDINGS_CHILD.equals(reader.getLocalName())) {
                  bindQueue();
               } else if (XmlDataConstants.ADDRESS_BINDINGS_CHILD.equals(reader.getLocalName())) {
                  bindAddress();
               } else if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName())) {
                  processMessage();
               }
            }
            reader.next();
         }

         if (sort) {
            for (XMLMessageImporter.MessageInfo msgtmp : messages) {
               sendMessage(msgtmp.queues, msgtmp.message, msgtmp.tempFile);
            }
         }

         if (!session.isAutoCommitSends()) {
            session.commit();
         }
      } finally {
         // if the session was created in our constructor then close it (otherwise the caller will close it)
         if (localSession) {
            session.close();
            managementSession.close();
         }
      }
   }

   private void processMessage() throws Exception {
      XMLMessageImporter.MessageInfo info = messageReader.readMessage(false);
      if (sort) {
         messages.add(info);
      } else {
         sendMessage(info.queues, info.message, info.tempFile);
      }
   }

   private void sendMessage(List<String> queues, Message message, File tempFileName) throws Exception {
      StringBuilder logMessage = new StringBuilder();
      String destination = addressMap.get(queues.get(0));

      logMessage.append("Sending ").append(message).append(" to address: ").append(destination).append("; routed to queues: ");
      ByteBuffer buffer = ByteBuffer.allocate(queues.size() * 8);

      for (String queue : queues) {
         long queueID;

         if (queueIDs.containsKey(queue)) {
            queueID = queueIDs.get(queue);
         } else {
            // Get the ID of the queues involved so the message can be routed properly.  This is done because we cannot
            // send directly to a queue, we have to send to an address instead but not all the queues related to the
            // address may need the message
            try (ClientRequestor requestor = new ClientRequestor(managementSession, "activemq.management")) {
               ClientMessage managementMessage = managementSession.createMessage(false);
               ManagementHelper.putAttribute(managementMessage, ResourceNames.QUEUE + queue, "ID");
               managementSession.start();
               if (logger.isDebugEnabled()) {
                  logger.debug("Requesting ID for: " + queue);
               }
               ClientMessage reply = requestor.request(managementMessage);
               Number idObject = (Number) ManagementHelper.getResult(reply);
               queueID = idObject.longValue();
            }
            if (logger.isDebugEnabled()) {
               logger.debug("ID for " + queue + " is: " + queueID);
            }
            queueIDs.put(queue, queueID);  // store it so we don't have to look it up every time
         }

         logMessage.append(queue).append(", ");
         buffer.putLong(queueID);
      }

      logMessage.delete(logMessage.length() - 2, logMessage.length()); // take off the trailing comma
      if (logger.isDebugEnabled()) {
         logger.debug(logMessage);
      }

      message.putBytesProperty(Message.HDR_ROUTE_TO_IDS, buffer.array());
      try (ClientProducer producer = session.createProducer(destination)) {
         producer.send(message);
      }

      if (tempFileName != null) {
         try {
            // this is to make sure the large message is sent before we delete it
            // to avoid races
            session.commit();
         } catch (Throwable dontcare) {
         }
         if (!tempFileName.delete()) {
            ActiveMQServerLogger.LOGGER.couldNotDeleteTempFile(tempFileName.getAbsolutePath());
         }
      }
   }

   private void oldBinding() throws Exception {
      String queueName = "";
      String address = "";
      String filter = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.OLD_ADDRESS:
               address = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.OLD_QUEUE:
               queueName = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.OLD_FILTER:
               filter = reader.getAttributeValue(i);
               break;
         }
      }

      if (queueName == null || address == null || filter == null) {
         // not expected to happen unless someone manually changed the format
         throw new IllegalStateException("invalid format, missing queue, address or filter");
      }

      RoutingType routingType = RoutingType.MULTICAST;

      if (address.startsWith(PacketImpl.OLD_QUEUE_PREFIX.toString())) {
         routingType = RoutingType.ANYCAST;
         if (!legacyPrefixes) {
            String newaddress = address.substring(PacketImpl.OLD_QUEUE_PREFIX.length());
            address = newaddress;
         }
      } else if (address.startsWith(PacketImpl.OLD_TOPIC_PREFIX.toString())) {
         routingType = RoutingType.MULTICAST;
         if (!legacyPrefixes) {
            String newaddress = address.substring(PacketImpl.OLD_TOPIC_PREFIX.length());
            address = newaddress;
         }
      }

      if (queueName.startsWith(PacketImpl.OLD_QUEUE_PREFIX.toString())) {
         if (!legacyPrefixes) {
            String newQueueName = queueName.substring(PacketImpl.OLD_QUEUE_PREFIX.length());
            oldPrefixTranslation.put(queueName, newQueueName);
            queueName = newQueueName;
         }
      } else if (queueName.startsWith(PacketImpl.OLD_TOPIC_PREFIX.toString())) {
         if (!legacyPrefixes) {
            String newQueueName = queueName.substring(PacketImpl.OLD_TOPIC_PREFIX.length());
            oldPrefixTranslation.put(queueName, newQueueName);
            queueName = newQueueName;
         }
      }


      ClientSession.AddressQuery addressQuery = session.addressQuery(SimpleString.toSimpleString(address));

      if (!addressQuery.isExists()) {
         session.createAddress(SimpleString.toSimpleString(address), routingType, true);
      }

      if (!filter.equals(FilterImpl.GENERIC_IGNORED_FILTER)) {
         ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString(queueName));

         if (!queueQuery.isExists()) {
            session.createQueue(address, routingType, queueName, filter, true);
            if (logger.isDebugEnabled()) {
               logger.debug("Binding queue(name=" + queueName + ", address=" + address + ", filter=" + filter + ")");
            }
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug("Binding " + queueName + " already exists so won't re-bind.");
            }
         }
      }

      addressMap.put(queueName, address);
   }


   private void bindQueue() throws Exception {
      String queueName = "";
      String address = "";
      String filter = "";
      String routingType = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.QUEUE_BINDING_ADDRESS:
               address = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.QUEUE_BINDING_NAME:
               queueName = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.QUEUE_BINDING_FILTER_STRING:
               filter = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.QUEUE_BINDING_ROUTING_TYPE:
               routingType = reader.getAttributeValue(i);
               break;
         }
      }

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString(queueName));

      if (!queueQuery.isExists()) {
         session.createQueue(address, RoutingType.valueOf(routingType), queueName, filter, true);
         if (logger.isDebugEnabled()) {
            logger.debug("Binding queue(name=" + queueName + ", address=" + address + ", filter=" + filter + ")");
         }
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Binding " + queueName + " already exists so won't re-bind.");
         }
      }

      addressMap.put(queueName, address);
   }

   private void bindAddress() throws Exception {
      String addressName = "";
      String routingTypes = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.ADDRESS_BINDING_NAME:
               addressName = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.ADDRESS_BINDING_ROUTING_TYPE:
               routingTypes = reader.getAttributeValue(i);
               break;
         }
      }

      ClientSession.AddressQuery addressQuery = session.addressQuery(new SimpleString(addressName));

      if (!addressQuery.isExists()) {
         EnumSet<RoutingType> set = EnumSet.noneOf(RoutingType.class);
         for (String routingType : ListUtil.toList(routingTypes)) {
            set.add(RoutingType.valueOf(routingType));
         }
         session.createAddress(SimpleString.toSimpleString(addressName), set, false);
         if (logger.isDebugEnabled()) {
            logger.debug("Binding address(name=" + addressName + ", routingTypes=" + routingTypes + ")");
         }
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Binding " + addressName + " already exists so won't re-bind.");
         }
      }
   }


}
