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
package org.apache.activemq.artemis.cli.commands.tools;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.Message;
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
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

/**
 * Read XML output from <code>org.apache.activemq.artemis.core.persistence.impl.journal.XmlDataExporter</code>, create a core session, and
 * send the messages to a running instance of ActiveMQ Artemis.  It uses the StAX <code>javax.xml.stream.XMLStreamReader</code>
 * for speed and simplicity.
 */
@Command(name = "imp", description = "Import all message-data using an XML that could be interpreted by any system.")
public final class XmlDataImporter extends ActionAbstract {
   // Constants -----------------------------------------------------

   private static final Logger logger = Logger.getLogger(XmlDataImporter.class);

   // Attributes ----------------------------------------------------

   private XMLStreamReader reader;

   // this session is really only needed if the "session" variable does not auto-commit sends
   ClientSession managementSession;

   boolean localSession;

   final Map<String, String> addressMap = new HashMap<>();

   final Map<String, Long> queueIDs = new HashMap<>();

   String tempFileName = "";

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
    * @throws Exception
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
      this.session = session;
      if (managementSession != null) {
         this.managementSession = managementSession;
      } else {
         this.managementSession = session;
      }
      localSession = false;

      processXml();

   }

   public void process(InputStream inputStream, String host, int port, boolean transactional) throws Exception {
      reader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
      HashMap<String, Object> connectionParams = new HashMap<>();
      connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
      connectionParams.put(TransportConstants.PORT_PROP_NAME, Integer.toString(port));
      ServerLocator serverLocator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams));
      ClientSessionFactory sf = serverLocator.createSessionFactory();

      if (user != null || password != null) {
         session = sf.createSession(user, password, false, !transactional, true, false, 0);
         managementSession = sf.createSession(user, password, false, true, true, false, 0);
      } else {
         session = sf.createSession(false, !transactional, true);
         managementSession = sf.createSession(false, true, true);
      }
      localSession = true;

      processXml();
   }

   private void processXml() throws Exception {
      try {
         while (reader.hasNext()) {
            if (logger.isDebugEnabled()) {
               logger.debug("EVENT:[" + reader.getLocation().getLineNumber() + "][" + reader.getLocation().getColumnNumber() + "] ");
            }
            if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
               if (XmlDataConstants.BINDINGS_CHILD.equals(reader.getLocalName())) {
                  bindQueue();
               } else if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName())) {
                  processMessage();
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORIES.equals(reader.getLocalName())) {
                  createJmsConnectionFactories();
               } else if (XmlDataConstants.JMS_DESTINATIONS.equals(reader.getLocalName())) {
                  createJmsDestinations();
               }
            }
            reader.next();
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
      Byte type = 0;
      Byte priority = 0;
      Long expiration = 0L;
      Long timestamp = 0L;
      org.apache.activemq.artemis.utils.UUID userId = null;
      ArrayList<String> queues = new ArrayList<>();

      // get message's attributes
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.MESSAGE_TYPE:
               type = getMessageType(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_PRIORITY:
               priority = Byte.parseByte(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_EXPIRATION:
               expiration = Long.parseLong(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_TIMESTAMP:
               timestamp = Long.parseLong(reader.getAttributeValue(i));
               break;
            case XmlDataConstants.MESSAGE_USER_ID:
               userId = UUIDGenerator.getInstance().generateUUID();
               break;
         }
      }

      Message message = session.createMessage(type, true, expiration, timestamp, priority);
      message.setUserID(userId);

      boolean endLoop = false;

      // loop through the XML and gather up all the message's data (i.e. body, properties, queues, etc.)
      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.MESSAGE_BODY.equals(reader.getLocalName())) {
                  processMessageBody(message);
               } else if (XmlDataConstants.PROPERTIES_CHILD.equals(reader.getLocalName())) {
                  processMessageProperties(message);
               } else if (XmlDataConstants.QUEUES_CHILD.equals(reader.getLocalName())) {
                  processMessageQueues(queues);
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }

      sendMessage(queues, message);
   }

   private Byte getMessageType(String value) {
      Byte type = Message.DEFAULT_TYPE;
      switch (value) {
         case XmlDataConstants.DEFAULT_TYPE_PRETTY:
            type = Message.DEFAULT_TYPE;
            break;
         case XmlDataConstants.BYTES_TYPE_PRETTY:
            type = Message.BYTES_TYPE;
            break;
         case XmlDataConstants.MAP_TYPE_PRETTY:
            type = Message.MAP_TYPE;
            break;
         case XmlDataConstants.OBJECT_TYPE_PRETTY:
            type = Message.OBJECT_TYPE;
            break;
         case XmlDataConstants.STREAM_TYPE_PRETTY:
            type = Message.STREAM_TYPE;
            break;
         case XmlDataConstants.TEXT_TYPE_PRETTY:
            type = Message.TEXT_TYPE;
            break;
      }
      return type;
   }

   private void sendMessage(ArrayList<String> queues, Message message) throws Exception {
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
            ClientRequestor requestor = new ClientRequestor(managementSession, "jms.queue.activemq.management");
            ClientMessage managementMessage = managementSession.createMessage(false);
            ManagementHelper.putAttribute(managementMessage, "core.queue." + queue, "ID");
            managementSession.start();
            if (logger.isDebugEnabled()) {
               logger.debug("Requesting ID for: " + queue);
            }
            ClientMessage reply = requestor.request(managementMessage);
            Number idObject = (Number) ManagementHelper.getResult(reply);
            queueID = idObject.longValue();
            requestor.close();
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

      message.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, buffer.array());
      try (ClientProducer producer = session.createProducer(destination)) {
         producer.send(message);
      }

      if (tempFileName.length() > 0) {
         File tempFile = new File(tempFileName);
         if (!tempFile.delete()) {
            ActiveMQServerLogger.LOGGER.couldNotDeleteTempFile(tempFileName);
         }
         tempFileName = "";
      }
   }

   private void processMessageQueues(ArrayList<String> queues) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         if (XmlDataConstants.QUEUE_NAME.equals(reader.getAttributeLocalName(i))) {
            queues.add(reader.getAttributeValue(i));
         }
      }
   }

   private void processMessageProperties(Message message) {
      String key = "";
      String value = "";
      String propertyType = "";
      String realStringValue = null;
      SimpleString realSimpleStringValue = null;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.PROPERTY_NAME:
               key = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.PROPERTY_VALUE:
               value = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.PROPERTY_TYPE:
               propertyType = reader.getAttributeValue(i);
               break;
         }
      }

      switch (propertyType) {
         case XmlDataConstants.PROPERTY_TYPE_SHORT:
            message.putShortProperty(key, Short.parseShort(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BOOLEAN:
            message.putBooleanProperty(key, Boolean.parseBoolean(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BYTE:
            message.putByteProperty(key, Byte.parseByte(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_BYTES:
            message.putBytesProperty(key, decode(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_DOUBLE:
            message.putDoubleProperty(key, Double.parseDouble(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_FLOAT:
            message.putFloatProperty(key, Float.parseFloat(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_INTEGER:
            message.putIntProperty(key, Integer.parseInt(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_LONG:
            message.putLongProperty(key, Long.parseLong(value));
            break;
         case XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING:
            if (!value.equals(XmlDataConstants.NULL)) {
               realSimpleStringValue = new SimpleString(value);
            }
            message.putStringProperty(new SimpleString(key), realSimpleStringValue);
            break;
         case XmlDataConstants.PROPERTY_TYPE_STRING:
            if (!value.equals(XmlDataConstants.NULL)) {
               realStringValue = value;
            }
            message.putStringProperty(key, realStringValue);
            break;
      }
   }

   private void processMessageBody(final Message message) throws XMLStreamException, IOException {
      boolean isLarge = false;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.MESSAGE_IS_LARGE.equals(attributeName)) {
            isLarge = Boolean.parseBoolean(reader.getAttributeValue(i));
         }
      }
      reader.next();
      if (logger.isDebugEnabled()) {
         logger.debug("XMLStreamReader impl: " + reader);
      }
      if (isLarge) {
         tempFileName = UUID.randomUUID().toString() + ".tmp";
         if (logger.isDebugEnabled()) {
            logger.debug("Creating temp file " + tempFileName + " for large message.");
         }
         try (OutputStream out = new FileOutputStream(tempFileName)) {
            getMessageBodyBytes(new MessageBodyBytesProcessor() {
               @Override
               public void processBodyBytes(byte[] bytes) throws IOException {
                  out.write(bytes);
               }
            });
         }
         FileInputStream fileInputStream = new FileInputStream(tempFileName);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
         ((ClientMessage) message).setBodyInputStream(bufferedInput);
      } else {
         getMessageBodyBytes(new MessageBodyBytesProcessor() {
            @Override
            public void processBodyBytes(byte[] bytes) throws IOException {
               message.getBodyBuffer().writeBytes(bytes);
            }
         });
      }
   }

   /**
    * Message bodies are written to XML as one or more Base64 encoded CDATA elements. Some parser implementations won't
    * read an entire CDATA element at once (e.g. Woodstox) so it's possible that multiple CDATA/CHARACTERS events need
    * to be combined to reconstruct the Base64 encoded string.  You can't decode bits and pieces of each CDATA.  Each
    * CDATA has to be decoded in its entirety.
    *
    * @param processor used to deal with the decoded CDATA elements
    * @throws IOException
    * @throws XMLStreamException
    */
   private void getMessageBodyBytes(MessageBodyBytesProcessor processor) throws IOException, XMLStreamException {
      int currentEventType;
      StringBuilder cdata = new StringBuilder();
      while (reader.hasNext()) {
         currentEventType = reader.getEventType();
         if (currentEventType == XMLStreamConstants.END_ELEMENT) {
            break;
         } else if (currentEventType == XMLStreamConstants.CHARACTERS && reader.isWhiteSpace() && cdata.length() > 0) {
         /* when we hit a whitespace CHARACTERS event we know that the entire CDATA is complete so decode, pass back to
          * the processor, and reset the cdata for the next event(s)
          */
            processor.processBodyBytes(decode(cdata.toString()));
            cdata.setLength(0);
         } else {
            cdata.append(new String(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength()).trim());
         }
         reader.next();
      }
   }

   private void bindQueue() throws Exception {
      String queueName = "";
      String address = "";
      String filter = "";

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String attributeName = reader.getAttributeLocalName(i);
         switch (attributeName) {
            case XmlDataConstants.BINDING_ADDRESS:
               address = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.BINDING_QUEUE_NAME:
               queueName = reader.getAttributeValue(i);
               break;
            case XmlDataConstants.BINDING_FILTER_STRING:
               filter = reader.getAttributeValue(i);
               break;
         }
      }

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString(queueName));

      if (!queueQuery.isExists()) {
         session.createQueue(address, queueName, filter, true);
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

   private void createJmsConnectionFactories() throws Exception {
      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORY.equals(reader.getLocalName())) {
                  createJmsConnectionFactory();
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORIES.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }
   }

   private void createJmsDestinations() throws Exception {
      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_DESTINATION.equals(reader.getLocalName())) {
                  createJmsDestination();
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_DESTINATIONS.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }
   }

   private void createJmsConnectionFactory() throws Exception {
      String name = "";
      String callFailoverTimeout = "";
      String callTimeout = "";
      String clientFailureCheckPeriod = "";
      String clientId = "";
      String confirmationWindowSize = "";
      String connectionTtl = "";
      String connectors = "";
      String consumerMaxRate = "";
      String consumerWindowSize = "";
      String discoveryGroupName = "";
      String dupsOkBatchSize = "";
      String groupId = "";
      String loadBalancingPolicyClassName = "";
      String maxRetryInterval = "";
      String minLargeMessageSize = "";
      String producerMaxRate = "";
      String producerWindowSize = "";
      String reconnectAttempts = "";
      String retryInterval = "";
      String retryIntervalMultiplier = "";
      String scheduledThreadMaxPoolSize = "";
      String threadMaxPoolSize = "";
      String transactionBatchSize = "";
      String type = "";
      String entries = "";
      String autoGroup = "";
      String blockOnAcknowledge = "";
      String blockOnDurableSend = "";
      String blockOnNonDurableSend = "";
      String cacheLargeMessagesClient = "";
      String compressLargeMessages = "";
      String failoverOnInitialConnection = "";
      String ha = "";
      String preacknowledge = "";
      String useGlobalPools = "";

      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORY_CALL_FAILOVER_TIMEOUT.equals(reader.getLocalName())) {
                  callFailoverTimeout = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory callFailoverTimeout: " + callFailoverTimeout);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CALL_TIMEOUT.equals(reader.getLocalName())) {
                  callTimeout = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory callTimeout: " + callTimeout);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CLIENT_FAILURE_CHECK_PERIOD.equals(reader.getLocalName())) {
                  clientFailureCheckPeriod = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory clientFailureCheckPeriod: " + clientFailureCheckPeriod);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CLIENT_ID.equals(reader.getLocalName())) {
                  clientId = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory clientId: " + clientId);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONFIRMATION_WINDOW_SIZE.equals(reader.getLocalName())) {
                  confirmationWindowSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory confirmationWindowSize: " + confirmationWindowSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTION_TTL.equals(reader.getLocalName())) {
                  connectionTtl = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory connectionTtl: " + connectionTtl);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTOR.equals(reader.getLocalName())) {
                  connectors = getConnectors();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory getLocalName: " + connectors);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONSUMER_MAX_RATE.equals(reader.getLocalName())) {
                  consumerMaxRate = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory consumerMaxRate: " + consumerMaxRate);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONSUMER_WINDOW_SIZE.equals(reader.getLocalName())) {
                  consumerWindowSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory consumerWindowSize: " + consumerWindowSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_DISCOVERY_GROUP_NAME.equals(reader.getLocalName())) {
                  discoveryGroupName = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory discoveryGroupName: " + discoveryGroupName);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_DUPS_OK_BATCH_SIZE.equals(reader.getLocalName())) {
                  dupsOkBatchSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory dupsOkBatchSize: " + dupsOkBatchSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_GROUP_ID.equals(reader.getLocalName())) {
                  groupId = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory groupId: " + groupId);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_LOAD_BALANCING_POLICY_CLASS_NAME.equals(reader.getLocalName())) {
                  loadBalancingPolicyClassName = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory loadBalancingPolicyClassName: " + loadBalancingPolicyClassName);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_MAX_RETRY_INTERVAL.equals(reader.getLocalName())) {
                  maxRetryInterval = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory maxRetryInterval: " + maxRetryInterval);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_MIN_LARGE_MESSAGE_SIZE.equals(reader.getLocalName())) {
                  minLargeMessageSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory minLargeMessageSize: " + minLargeMessageSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_NAME.equals(reader.getLocalName())) {
                  name = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory name: " + name);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_PRODUCER_MAX_RATE.equals(reader.getLocalName())) {
                  producerMaxRate = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory producerMaxRate: " + producerMaxRate);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_PRODUCER_WINDOW_SIZE.equals(reader.getLocalName())) {
                  producerWindowSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory producerWindowSize: " + producerWindowSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_RECONNECT_ATTEMPTS.equals(reader.getLocalName())) {
                  reconnectAttempts = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory reconnectAttempts: " + reconnectAttempts);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_RETRY_INTERVAL.equals(reader.getLocalName())) {
                  retryInterval = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory retryInterval: " + retryInterval);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_RETRY_INTERVAL_MULTIPLIER.equals(reader.getLocalName())) {
                  retryIntervalMultiplier = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory retryIntervalMultiplier: " + retryIntervalMultiplier);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_SCHEDULED_THREAD_POOL_MAX_SIZE.equals(reader.getLocalName())) {
                  scheduledThreadMaxPoolSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory scheduledThreadMaxPoolSize: " + scheduledThreadMaxPoolSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_THREAD_POOL_MAX_SIZE.equals(reader.getLocalName())) {
                  threadMaxPoolSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory threadMaxPoolSize: " + threadMaxPoolSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_TRANSACTION_BATCH_SIZE.equals(reader.getLocalName())) {
                  transactionBatchSize = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory transactionBatchSize: " + transactionBatchSize);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_TYPE.equals(reader.getLocalName())) {
                  type = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory type: " + type);
                  }
               } else if (XmlDataConstants.JMS_JNDI_ENTRIES.equals(reader.getLocalName())) {
                  entries = getEntries();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory entries: " + entries);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_AUTO_GROUP.equals(reader.getLocalName())) {
                  autoGroup = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory autoGroup: " + autoGroup);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_ACKNOWLEDGE.equals(reader.getLocalName())) {
                  blockOnAcknowledge = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory blockOnAcknowledge: " + blockOnAcknowledge);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_DURABLE_SEND.equals(reader.getLocalName())) {
                  blockOnDurableSend = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory blockOnDurableSend: " + blockOnDurableSend);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_NON_DURABLE_SEND.equals(reader.getLocalName())) {
                  blockOnNonDurableSend = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory blockOnNonDurableSend: " + blockOnNonDurableSend);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_CACHE_LARGE_MESSAGES_CLIENT.equals(reader.getLocalName())) {
                  cacheLargeMessagesClient = reader.getElementText();
                  ActiveMQServerLogger.LOGGER.info("JMS connection factory " + name + " cacheLargeMessagesClient: " + cacheLargeMessagesClient);
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_COMPRESS_LARGE_MESSAGES.equals(reader.getLocalName())) {
                  compressLargeMessages = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory compressLargeMessages: " + compressLargeMessages);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_FAILOVER_ON_INITIAL_CONNECTION.equals(reader.getLocalName())) {
                  failoverOnInitialConnection = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory failoverOnInitialConnection: " + failoverOnInitialConnection);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_HA.equals(reader.getLocalName())) {
                  ha = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory ha: " + ha);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_PREACKNOWLEDGE.equals(reader.getLocalName())) {
                  preacknowledge = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory preacknowledge: " + preacknowledge);
                  }
               } else if (XmlDataConstants.JMS_CONNECTION_FACTORY_USE_GLOBAL_POOLS.equals(reader.getLocalName())) {
                  useGlobalPools = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS connection factory useGlobalPools: " + useGlobalPools);
                  }
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORY.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }

      ClientRequestor requestor = new ClientRequestor(managementSession, "jms.queue.activemq.management");
      ClientMessage managementMessage = managementSession.createMessage(false);
      ManagementHelper.putOperationInvocation(managementMessage, ResourceNames.JMS_SERVER, "createConnectionFactory", name, Boolean.parseBoolean(ha), discoveryGroupName.length() > 0, Integer.parseInt(type), connectors, entries, clientId, Long.parseLong(clientFailureCheckPeriod), Long.parseLong(connectionTtl), Long.parseLong(callTimeout), Long.parseLong(callFailoverTimeout), Integer.parseInt(minLargeMessageSize), Boolean.parseBoolean(compressLargeMessages), Integer.parseInt(consumerWindowSize), Integer.parseInt(consumerMaxRate), Integer.parseInt(confirmationWindowSize), Integer.parseInt(producerWindowSize), Integer.parseInt(producerMaxRate), Boolean.parseBoolean(blockOnAcknowledge), Boolean.parseBoolean(blockOnDurableSend), Boolean.parseBoolean(blockOnNonDurableSend), Boolean.parseBoolean(autoGroup), Boolean.parseBoolean(preacknowledge), loadBalancingPolicyClassName, Integer.parseInt(transactionBatchSize), Integer.parseInt(dupsOkBatchSize), Boolean.parseBoolean(useGlobalPools), Integer.parseInt(scheduledThreadMaxPoolSize), Integer.parseInt(threadMaxPoolSize), Long.parseLong(retryInterval), Double.parseDouble(retryIntervalMultiplier), Long.parseLong(maxRetryInterval), Integer.parseInt(reconnectAttempts), Boolean.parseBoolean(failoverOnInitialConnection), groupId);
      //Boolean.parseBoolean(cacheLargeMessagesClient));
      managementSession.start();
      ClientMessage reply = requestor.request(managementMessage);
      if (ManagementHelper.hasOperationSucceeded(reply)) {
         if (logger.isDebugEnabled()) {
            logger.debug("Created connection factory " + name);
         }
      } else {
         ActiveMQServerLogger.LOGGER.error("Problem creating " + name);
      }

      requestor.close();
   }

   private void createJmsDestination() throws Exception {
      String name = "";
      String selector = "";
      String entries = "";
      String type = "";
      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_DESTINATION_NAME.equals(reader.getLocalName())) {
                  name = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS destination name: " + name);
                  }
               } else if (XmlDataConstants.JMS_DESTINATION_SELECTOR.equals(reader.getLocalName())) {
                  selector = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS destination selector: " + selector);
                  }
               } else if (XmlDataConstants.JMS_DESTINATION_TYPE.equals(reader.getLocalName())) {
                  type = reader.getElementText();
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS destination type: " + type);
                  }
               } else if (XmlDataConstants.JMS_JNDI_ENTRIES.equals(reader.getLocalName())) {
                  entries = getEntries();
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_DESTINATION.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }

      ClientRequestor requestor = new ClientRequestor(managementSession, "jms.queue.activemq.management");
      ClientMessage managementMessage = managementSession.createMessage(false);
      if ("Queue".equals(type)) {
         ManagementHelper.putOperationInvocation(managementMessage, ResourceNames.JMS_SERVER, "createQueue", name, entries, selector);
      } else if ("Topic".equals(type)) {
         ManagementHelper.putOperationInvocation(managementMessage, ResourceNames.JMS_SERVER, "createTopic", name, entries);
      }
      managementSession.start();
      ClientMessage reply = requestor.request(managementMessage);
      if (ManagementHelper.hasOperationSucceeded(reply)) {
         if (logger.isDebugEnabled()) {
            logger.debug("Created " + type.toLowerCase() + " " + name);
         }
      } else {
         ActiveMQServerLogger.LOGGER.error("Problem creating " + name);
      }

      requestor.close();
   }

   private String getEntries() throws Exception {
      StringBuilder entry = new StringBuilder();
      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_JNDI_ENTRY.equals(reader.getLocalName())) {
                  String elementText = reader.getElementText();
                  entry.append(elementText).append(", ");
                  if (logger.isDebugEnabled()) {
                     logger.debug("JMS admin object JNDI entry: " + entry.toString());
                  }
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_JNDI_ENTRIES.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }

      return entry.delete(entry.length() - 2, entry.length()).toString();
   }

   private String getConnectors() throws Exception {
      StringBuilder entry = new StringBuilder();
      boolean endLoop = false;

      while (reader.hasNext()) {
         int eventType = reader.getEventType();
         switch (eventType) {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTOR.equals(reader.getLocalName())) {
                  entry.append(reader.getElementText()).append(", ");
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTORS.equals(reader.getLocalName())) {
                  endLoop = true;
               }
               break;
         }
         if (endLoop) {
            break;
         }
         reader.next();
      }

      return entry.delete(entry.length() - 2, entry.length()).toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] decode(String data) {
      return Base64.decode(data, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   private interface MessageBodyBytesProcessor {

      void processBodyBytes(byte[] bytes) throws IOException;
   }

   // Inner classes -------------------------------------------------

}
