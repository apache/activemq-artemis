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

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.TransactionFailureCallback;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.message.BodyEncoder;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.impl.PagePositionImpl;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PageTransactionInfoImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal.MessageDescribe;
import org.apache.activemq.artemis.core.persistence.impl.journal.DescribeJournal.ReferenceDescribe;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.AckDescribe;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.CursorAckRecordEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PageUpdateTXEncoding;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.PersistentQueueBindingEncoding;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.jms.persistence.config.PersistedBindings;
import org.apache.activemq.artemis.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq.artemis.jms.persistence.config.PersistedDestination;
import org.apache.activemq.artemis.jms.persistence.config.PersistedType;
import org.apache.activemq.artemis.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.ExecutorFactory;

@Command(name = "exp", description = "Export all message-data using an XML that could be interpreted by any system.")
public final class XmlDataExporter extends LockAbstract {

   private static final Long LARGE_MESSAGE_CHUNK_SIZE = 1000L;

   private JournalStorageManager storageManager;

   private Configuration config;

   private XMLStreamWriter xmlWriter;

   // an inner map of message refs hashed by the queue ID to which they belong and then hashed by their record ID
   private final Map<Long, HashMap<Long, ReferenceDescribe>> messageRefs = new HashMap<>();

   // map of all message records hashed by their record ID (which will match the record ID of the message refs)
   private final HashMap<Long, Message> messages = new HashMap<>();

   private final Map<Long, Set<PagePosition>> cursorRecords = new HashMap<>();

   private final Set<Long> pgTXs = new HashSet<>();

   private final HashMap<Long, PersistentQueueBindingEncoding> queueBindings = new HashMap<>();

   private final Map<String, PersistedConnectionFactory> jmsConnectionFactories = new ConcurrentHashMap<>();

   private final Map<Pair<PersistedType, String>, PersistedDestination> jmsDestinations = new ConcurrentHashMap<>();

   private final Map<Pair<PersistedType, String>, PersistedBindings> jmsJNDI = new ConcurrentHashMap<>();

   long messagesPrinted = 0L;

   long bindingsPrinted = 0L;

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      try {
         process(context.out, getBinding(), getJournal(), getPaging(), getLargeMessages());
      }
      catch (Exception e) {
         treatError(e, "data", "exp");
      }
      return null;
   }

   public void process(OutputStream out,
                       String bindingsDir,
                       String journalDir,
                       String pagingDir,
                       String largeMessagesDir) throws Exception {
      config = new ConfigurationImpl().setBindingsDirectory(bindingsDir).setJournalDirectory(journalDir).setPagingDirectory(pagingDir).setLargeMessagesDirectory(largeMessagesDir).setJournalType(JournalType.NIO);
      final ExecutorService executor = Executors.newFixedThreadPool(1);
      ExecutorFactory executorFactory = new ExecutorFactory() {
         @Override
         public Executor getExecutor() {
            return executor;
         }
      };

      storageManager = new JournalStorageManager(config, executorFactory);

      XMLOutputFactory factory = XMLOutputFactory.newInstance();
      XMLStreamWriter rawXmlWriter = factory.createXMLStreamWriter(out, "UTF-8");
      PrettyPrintHandler handler = new PrettyPrintHandler(rawXmlWriter);
      xmlWriter = (XMLStreamWriter) Proxy.newProxyInstance(XMLStreamWriter.class.getClassLoader(), new Class[]{XMLStreamWriter.class}, handler);

      writeXMLData();
   }

   private void writeXMLData() throws Exception {
      long start = System.currentTimeMillis();
      getBindings();
      getJmsBindings();
      processMessageJournal();
      printDataAsXML();
      ActiveMQServerLogger.LOGGER.debug("\n\nProcessing took: " + (System.currentTimeMillis() - start) + "ms");
      ActiveMQServerLogger.LOGGER.debug("Output " + messagesPrinted + " messages and " + bindingsPrinted + " bindings.");
   }

   /**
    * Read through the message journal and stuff all the events/data we care about into local data structures.  We'll
    * use this data later to print all the right information.
    *
    * @throws Exception will be thrown if anything goes wrong reading the journal
    */
   private void processMessageJournal() throws Exception {
      ArrayList<RecordInfo> acks = new ArrayList<>();

      List<RecordInfo> records = new LinkedList<>();

      // We load these, but don't use them.
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<>();

      Journal messageJournal = storageManager.getMessageJournal();

      ActiveMQServerLogger.LOGGER.debug("Reading journal from " + config.getJournalDirectory());

      messageJournal.start();

      // Just logging these, no action necessary
      TransactionFailureCallback transactionFailureCallback = new TransactionFailureCallback() {
         @Override
         public void failedTransaction(long transactionID,
                                       List<RecordInfo> records1,
                                       List<RecordInfo> recordsToDelete) {
            StringBuilder message = new StringBuilder();
            message.append("Encountered failed journal transaction: ").append(transactionID);
            for (int i = 0; i < records1.size(); i++) {
               if (i == 0) {
                  message.append("; Records: ");
               }
               message.append(records1.get(i));
               if (i != (records1.size() - 1)) {
                  message.append(", ");
               }
            }

            for (int i = 0; i < recordsToDelete.size(); i++) {
               if (i == 0) {
                  message.append("; RecordsToDelete: ");
               }
               message.append(recordsToDelete.get(i));
               if (i != (recordsToDelete.size() - 1)) {
                  message.append(", ");
               }
            }

            ActiveMQServerLogger.LOGGER.debug(message.toString());
         }
      };

      ((JournalImpl) messageJournal).load(records, preparedTransactions, transactionFailureCallback, false);

      // Since we don't use these nullify the reference so that the garbage collector can clean them up
      preparedTransactions = null;

      for (RecordInfo info : records) {
         byte[] data = info.data;

         ActiveMQBuffer buff = ActiveMQBuffers.wrappedBuffer(data);

         Object o = DescribeJournal.newObjectEncoding(info, storageManager);
         if (info.getUserRecordType() == JournalRecordIds.ADD_MESSAGE) {
            messages.put(info.id, ((MessageDescribe) o).getMsg());
         }
         else if (info.getUserRecordType() == JournalRecordIds.ADD_LARGE_MESSAGE) {
            messages.put(info.id, ((MessageDescribe) o).getMsg());
         }
         else if (info.getUserRecordType() == JournalRecordIds.ADD_REF) {
            ReferenceDescribe ref = (ReferenceDescribe) o;
            HashMap<Long, ReferenceDescribe> map = messageRefs.get(info.id);
            if (map == null) {
               HashMap<Long, ReferenceDescribe> newMap = new HashMap<>();
               newMap.put(ref.refEncoding.queueID, ref);
               messageRefs.put(info.id, newMap);
            }
            else {
               map.put(ref.refEncoding.queueID, ref);
            }
         }
         else if (info.getUserRecordType() == JournalRecordIds.ACKNOWLEDGE_REF) {
            acks.add(info);
         }
         else if (info.userRecordType == JournalRecordIds.ACKNOWLEDGE_CURSOR) {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
            encoding.decode(buff);

            Set<PagePosition> set = cursorRecords.get(encoding.queueID);

            if (set == null) {
               set = new HashSet<>();
               cursorRecords.put(encoding.queueID, set);
            }

            set.add(encoding.position);
         }
         else if (info.userRecordType == JournalRecordIds.PAGE_TRANSACTION) {
            if (info.isUpdate) {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buff);
               pgTXs.add(pageUpdate.pageTX);
            }
            else {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(info.id);
               pgTXs.add(pageTransactionInfo.getTransactionID());
            }
         }
      }

      messageJournal.stop();

      removeAcked(acks);
   }

   /**
    * Go back through the messages and message refs we found in the journal and remove the ones that have been acked.
    *
    * @param acks the list of ack records we got from the journal
    */
   private void removeAcked(ArrayList<RecordInfo> acks) {
      for (RecordInfo info : acks) {
         AckDescribe ack = (AckDescribe) DescribeJournal.newObjectEncoding(info, null);
         HashMap<Long, ReferenceDescribe> referenceDescribeHashMap = messageRefs.get(info.id);
         referenceDescribeHashMap.remove(ack.refEncoding.queueID);
         if (referenceDescribeHashMap.size() == 0) {
            messages.remove(info.id);
            messageRefs.remove(info.id);
         }
      }
   }

   private void getJmsBindings() throws Exception {
      SequentialFileFactory bindingsJMS = new NIOSequentialFileFactory(config.getBindingsLocation(), 1);

      Journal jmsJournal = new JournalImpl(1024 * 1024, 2, 2, config.getJournalCompactMinFiles(), config.getJournalCompactPercentage(), bindingsJMS, "activemq-jms", "jms", 1);

      jmsJournal.start();

      List<RecordInfo> data = new ArrayList<>();

      ArrayList<PreparedTransactionInfo> list = new ArrayList<>();

      ActiveMQServerLogger.LOGGER.debug("Reading jms bindings journal from " + config.getBindingsDirectory());

      jmsJournal.load(data, list, null);

      for (RecordInfo record : data) {
         long id = record.id;

         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();

         if (rec == JMSJournalStorageManagerImpl.CF_RECORD) {
            PersistedConnectionFactory cf = new PersistedConnectionFactory();
            cf.decode(buffer);
            cf.setId(id);
            ActiveMQServerLogger.LOGGER.info("Found JMS connection factory: " + cf.getName());
            jmsConnectionFactories.put(cf.getName(), cf);
         }
         else if (rec == JMSJournalStorageManagerImpl.DESTINATION_RECORD) {
            PersistedDestination destination = new PersistedDestination();
            destination.decode(buffer);
            destination.setId(id);
            ActiveMQServerLogger.LOGGER.info("Found JMS destination: " + destination.getName());
            jmsDestinations.put(new Pair<>(destination.getType(), destination.getName()), destination);
         }
         else if (rec == JMSJournalStorageManagerImpl.BINDING_RECORD) {
            PersistedBindings jndi = new PersistedBindings();
            jndi.decode(buffer);
            jndi.setId(id);
            Pair<PersistedType, String> key = new Pair<>(jndi.getType(), jndi.getName());
            StringBuilder builder = new StringBuilder();
            for (String binding : jndi.getBindings()) {
               builder.append(binding).append(" ");
            }
            ActiveMQServerLogger.LOGGER.info("Found JMS JNDI binding data for " + jndi.getType() + " " + jndi.getName() + ": " + builder.toString());
            jmsJNDI.put(key, jndi);
         }
         else {
            throw new IllegalStateException("Invalid record type " + rec);
         }

      }
   }

   /**
    * Open the bindings journal and extract all bindings data.
    *
    * @throws Exception will be thrown if anything goes wrong reading the bindings journal
    */
   private void getBindings() throws Exception {
      List<RecordInfo> records = new LinkedList<>();

      Journal bindingsJournal = storageManager.getBindingsJournal();

      bindingsJournal.start();

      ActiveMQServerLogger.LOGGER.debug("Reading bindings journal from " + config.getBindingsDirectory());

      ((JournalImpl) bindingsJournal).load(records, null, null, false);

      for (RecordInfo info : records) {
         if (info.getUserRecordType() == JournalRecordIds.QUEUE_BINDING_RECORD) {
            PersistentQueueBindingEncoding bindingEncoding = (PersistentQueueBindingEncoding) DescribeJournal.newObjectEncoding(info, null);
            queueBindings.put(bindingEncoding.getId(), bindingEncoding);
         }
      }

      bindingsJournal.stop();
   }

   private void printDataAsXML() {
      try {
         xmlWriter.writeStartDocument(XmlDataConstants.XML_VERSION);
         xmlWriter.writeStartElement(XmlDataConstants.DOCUMENT_PARENT);
         printBindingsAsXML();
         printJmsConnectionFactoriesAsXML();
         printJmsDestinationsAsXML();
         printAllMessagesAsXML();
         xmlWriter.writeEndElement(); // end DOCUMENT_PARENT
         xmlWriter.writeEndDocument();
         xmlWriter.flush();
         xmlWriter.close();
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void printBindingsAsXML() throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.BINDINGS_PARENT);
      for (Map.Entry<Long, PersistentQueueBindingEncoding> queueBindingEncodingEntry : queueBindings.entrySet()) {
         PersistentQueueBindingEncoding bindingEncoding = queueBindings.get(queueBindingEncodingEntry.getKey());
         xmlWriter.writeEmptyElement(XmlDataConstants.BINDINGS_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_ADDRESS, bindingEncoding.getAddress().toString());
         String filter = "";
         if (bindingEncoding.getFilterString() != null) {
            filter = bindingEncoding.getFilterString().toString();
         }
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_FILTER_STRING, filter);
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_QUEUE_NAME, bindingEncoding.getQueueName().toString());
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_ID, Long.toString(bindingEncoding.getId()));
         bindingsPrinted++;
      }
      xmlWriter.writeEndElement(); // end BINDINGS_PARENT
   }

   private void printJmsConnectionFactoriesAsXML() throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORIES);
      for (String jmsConnectionFactoryKey : jmsConnectionFactories.keySet()) {
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY);
         PersistedConnectionFactory jmsConnectionFactory = jmsConnectionFactories.get(jmsConnectionFactoryKey);
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_NAME);
         xmlWriter.writeCharacters(jmsConnectionFactory.getName());
         xmlWriter.writeEndElement();
         String clientID = jmsConnectionFactory.getConfig().getClientID();
         if (clientID != null) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CLIENT_ID);
            xmlWriter.writeCharacters(clientID);
            xmlWriter.writeEndElement();
         }

         long callFailoverTimeout = jmsConnectionFactory.getConfig().getCallFailoverTimeout();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CALL_FAILOVER_TIMEOUT);
         xmlWriter.writeCharacters(Long.toString(callFailoverTimeout));
         xmlWriter.writeEndElement();

         long callTimeout = jmsConnectionFactory.getConfig().getCallTimeout();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CALL_TIMEOUT);
         xmlWriter.writeCharacters(Long.toString(callTimeout));
         xmlWriter.writeEndElement();

         long clientFailureCheckPeriod = jmsConnectionFactory.getConfig().getClientFailureCheckPeriod();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CLIENT_FAILURE_CHECK_PERIOD);
         xmlWriter.writeCharacters(Long.toString(clientFailureCheckPeriod));
         xmlWriter.writeEndElement();

         int confirmationWindowSize = jmsConnectionFactory.getConfig().getConfirmationWindowSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONFIRMATION_WINDOW_SIZE);
         xmlWriter.writeCharacters(Integer.toString(confirmationWindowSize));
         xmlWriter.writeEndElement();

         long connectionTTL = jmsConnectionFactory.getConfig().getConnectionTTL();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTION_TTL);
         xmlWriter.writeCharacters(Long.toString(connectionTTL));
         xmlWriter.writeEndElement();

         long consumerMaxRate = jmsConnectionFactory.getConfig().getConsumerMaxRate();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONSUMER_MAX_RATE);
         xmlWriter.writeCharacters(Long.toString(consumerMaxRate));
         xmlWriter.writeEndElement();

         long consumerWindowSize = jmsConnectionFactory.getConfig().getConsumerWindowSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONSUMER_WINDOW_SIZE);
         xmlWriter.writeCharacters(Long.toString(consumerWindowSize));
         xmlWriter.writeEndElement();

         String discoveryGroupName = jmsConnectionFactory.getConfig().getDiscoveryGroupName();
         if (discoveryGroupName != null) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_DISCOVERY_GROUP_NAME);
            xmlWriter.writeCharacters(discoveryGroupName);
            xmlWriter.writeEndElement();
         }

         int dupsOKBatchSize = jmsConnectionFactory.getConfig().getDupsOKBatchSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_DUPS_OK_BATCH_SIZE);
         xmlWriter.writeCharacters(Integer.toString(dupsOKBatchSize));
         xmlWriter.writeEndElement();

         JMSFactoryType factoryType = jmsConnectionFactory.getConfig().getFactoryType();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_TYPE);
         xmlWriter.writeCharacters(Integer.toString(factoryType.intValue()));
         xmlWriter.writeEndElement();

         String groupID = jmsConnectionFactory.getConfig().getGroupID();
         if (groupID != null) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_GROUP_ID);
            xmlWriter.writeCharacters(groupID);
            xmlWriter.writeEndElement();
         }

         String loadBalancingPolicyClassName = jmsConnectionFactory.getConfig().getLoadBalancingPolicyClassName();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_LOAD_BALANCING_POLICY_CLASS_NAME);
         xmlWriter.writeCharacters(loadBalancingPolicyClassName);
         xmlWriter.writeEndElement();

         long maxRetryInterval = jmsConnectionFactory.getConfig().getMaxRetryInterval();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_MAX_RETRY_INTERVAL);
         xmlWriter.writeCharacters(Long.toString(maxRetryInterval));
         xmlWriter.writeEndElement();

         long minLargeMessageSize = jmsConnectionFactory.getConfig().getMinLargeMessageSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_MIN_LARGE_MESSAGE_SIZE);
         xmlWriter.writeCharacters(Long.toString(minLargeMessageSize));
         xmlWriter.writeEndElement();

         long producerMaxRate = jmsConnectionFactory.getConfig().getProducerMaxRate();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_PRODUCER_MAX_RATE);
         xmlWriter.writeCharacters(Long.toString(producerMaxRate));
         xmlWriter.writeEndElement();

         long producerWindowSize = jmsConnectionFactory.getConfig().getProducerWindowSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_PRODUCER_WINDOW_SIZE);
         xmlWriter.writeCharacters(Long.toString(producerWindowSize));
         xmlWriter.writeEndElement();

         long reconnectAttempts = jmsConnectionFactory.getConfig().getReconnectAttempts();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_RECONNECT_ATTEMPTS);
         xmlWriter.writeCharacters(Long.toString(reconnectAttempts));
         xmlWriter.writeEndElement();

         long retryInterval = jmsConnectionFactory.getConfig().getRetryInterval();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_RETRY_INTERVAL);
         xmlWriter.writeCharacters(Long.toString(retryInterval));
         xmlWriter.writeEndElement();

         double retryIntervalMultiplier = jmsConnectionFactory.getConfig().getRetryIntervalMultiplier();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_RETRY_INTERVAL_MULTIPLIER);
         xmlWriter.writeCharacters(Double.toString(retryIntervalMultiplier));
         xmlWriter.writeEndElement();

         long scheduledThreadPoolMaxSize = jmsConnectionFactory.getConfig().getScheduledThreadPoolMaxSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_SCHEDULED_THREAD_POOL_MAX_SIZE);
         xmlWriter.writeCharacters(Long.toString(scheduledThreadPoolMaxSize));
         xmlWriter.writeEndElement();

         long threadPoolMaxSize = jmsConnectionFactory.getConfig().getThreadPoolMaxSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_THREAD_POOL_MAX_SIZE);
         xmlWriter.writeCharacters(Long.toString(threadPoolMaxSize));
         xmlWriter.writeEndElement();

         long transactionBatchSize = jmsConnectionFactory.getConfig().getTransactionBatchSize();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_TRANSACTION_BATCH_SIZE);
         xmlWriter.writeCharacters(Long.toString(transactionBatchSize));
         xmlWriter.writeEndElement();

         boolean autoGroup = jmsConnectionFactory.getConfig().isAutoGroup();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_AUTO_GROUP);
         xmlWriter.writeCharacters(Boolean.toString(autoGroup));
         xmlWriter.writeEndElement();

         boolean blockOnAcknowledge = jmsConnectionFactory.getConfig().isBlockOnAcknowledge();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_ACKNOWLEDGE);
         xmlWriter.writeCharacters(Boolean.toString(blockOnAcknowledge));
         xmlWriter.writeEndElement();

         boolean blockOnDurableSend = jmsConnectionFactory.getConfig().isBlockOnDurableSend();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_DURABLE_SEND);
         xmlWriter.writeCharacters(Boolean.toString(blockOnDurableSend));
         xmlWriter.writeEndElement();

         boolean blockOnNonDurableSend = jmsConnectionFactory.getConfig().isBlockOnNonDurableSend();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_BLOCK_ON_NON_DURABLE_SEND);
         xmlWriter.writeCharacters(Boolean.toString(blockOnNonDurableSend));
         xmlWriter.writeEndElement();

         boolean cacheLargeMessagesClient = jmsConnectionFactory.getConfig().isCacheLargeMessagesClient();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CACHE_LARGE_MESSAGES_CLIENT);
         xmlWriter.writeCharacters(Boolean.toString(cacheLargeMessagesClient));
         xmlWriter.writeEndElement();

         boolean compressLargeMessages = jmsConnectionFactory.getConfig().isCompressLargeMessages();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_COMPRESS_LARGE_MESSAGES);
         xmlWriter.writeCharacters(Boolean.toString(compressLargeMessages));
         xmlWriter.writeEndElement();

         boolean failoverOnInitialConnection = jmsConnectionFactory.getConfig().isFailoverOnInitialConnection();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_FAILOVER_ON_INITIAL_CONNECTION);
         xmlWriter.writeCharacters(Boolean.toString(failoverOnInitialConnection));
         xmlWriter.writeEndElement();

         boolean ha = jmsConnectionFactory.getConfig().isHA();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_HA);
         xmlWriter.writeCharacters(Boolean.toString(ha));
         xmlWriter.writeEndElement();

         boolean preAcknowledge = jmsConnectionFactory.getConfig().isPreAcknowledge();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_PREACKNOWLEDGE);
         xmlWriter.writeCharacters(Boolean.toString(preAcknowledge));
         xmlWriter.writeEndElement();

         boolean useGlobalPools = jmsConnectionFactory.getConfig().isUseGlobalPools();
         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_USE_GLOBAL_POOLS);
         xmlWriter.writeCharacters(Boolean.toString(useGlobalPools));
         xmlWriter.writeEndElement();

         xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTORS);
         for (String connector : jmsConnectionFactory.getConfig().getConnectorNames()) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_CONNECTION_FACTORY_CONNECTOR);
            xmlWriter.writeCharacters(connector);
            xmlWriter.writeEndElement();
         }
         xmlWriter.writeEndElement();

         xmlWriter.writeStartElement(XmlDataConstants.JMS_JNDI_ENTRIES);
         PersistedBindings jndi = jmsJNDI.get(new Pair<>(PersistedType.ConnectionFactory, jmsConnectionFactory.getName()));
         for (String jndiEntry : jndi.getBindings()) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_JNDI_ENTRY);
            xmlWriter.writeCharacters(jndiEntry);
            xmlWriter.writeEndElement();
         }
         xmlWriter.writeEndElement(); // end jndi-entries
         xmlWriter.writeEndElement(); // end JMS_CONNECTION_FACTORY
      }
      xmlWriter.writeEndElement();
   }

   private void printJmsDestinationsAsXML() throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.JMS_DESTINATIONS);
      for (Pair<PersistedType, String> jmsDestinationsKey : jmsDestinations.keySet()) {
         PersistedDestination jmsDestination = jmsDestinations.get(jmsDestinationsKey);
         xmlWriter.writeStartElement(XmlDataConstants.JMS_DESTINATION);

         xmlWriter.writeStartElement(XmlDataConstants.JMS_DESTINATION_NAME);
         xmlWriter.writeCharacters(jmsDestination.getName());
         xmlWriter.writeEndElement();

         String selector = jmsDestination.getSelector();
         if (selector != null && selector.length() != 0) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_DESTINATION_SELECTOR);
            xmlWriter.writeCharacters(selector);
            xmlWriter.writeEndElement();
         }

         xmlWriter.writeStartElement(XmlDataConstants.JMS_DESTINATION_TYPE);
         xmlWriter.writeCharacters(jmsDestination.getType().toString());
         xmlWriter.writeEndElement();

         xmlWriter.writeStartElement(XmlDataConstants.JMS_JNDI_ENTRIES);
         PersistedBindings jndi = jmsJNDI.get(new Pair<>(jmsDestination.getType(), jmsDestination.getName()));
         for (String jndiEntry : jndi.getBindings()) {
            xmlWriter.writeStartElement(XmlDataConstants.JMS_JNDI_ENTRY);
            xmlWriter.writeCharacters(jndiEntry);
            xmlWriter.writeEndElement();
         }
         xmlWriter.writeEndElement(); // end jndi-entries
         xmlWriter.writeEndElement(); // end JMS_CONNECTION_FACTORY
      }
      xmlWriter.writeEndElement();
   }

   private void printAllMessagesAsXML() throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGES_PARENT);

      // Order here is important.  We must process the messages from the journal before we process those from the page
      // files in order to get the messages in the right order.
      for (Map.Entry<Long, Message> messageMapEntry : messages.entrySet()) {
         printSingleMessageAsXML((ServerMessage) messageMapEntry.getValue(), extractQueueNames(messageRefs.get(messageMapEntry.getKey())));
      }

      printPagedMessagesAsXML();

      xmlWriter.writeEndElement(); // end "messages"
   }

   /**
    * Reads from the page files and prints messages as it finds them (making sure to check acks and transactions
    * from the journal).
    */
   private void printPagedMessagesAsXML() {
      try {
         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
         final ExecutorService executor = Executors.newFixedThreadPool(10);
         ExecutorFactory executorFactory = new ExecutorFactory() {
            @Override
            public Executor getExecutor() {
               return executor;
            }
         };
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(storageManager, config.getPagingLocation(), 1000L, scheduled, executorFactory, true, null);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<>();
         addressSettingsRepository.setDefault(new AddressSettings());
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, addressSettingsRepository);

         manager.start();

         SimpleString[] stores = manager.getStoreNames();

         for (SimpleString store : stores) {
            PagingStore pageStore = manager.getPageStore(store);

            if (pageStore != null) {
               File folder = pageStore.getFolder();
               ActiveMQServerLogger.LOGGER.debug("Reading page store " + store + " folder = " + folder);

               int pageId = (int) pageStore.getFirstPage();
               for (int i = 0; i < pageStore.getNumberOfPages(); i++) {
                  ActiveMQServerLogger.LOGGER.debug("Reading page " + pageId);
                  Page page = pageStore.createPage(pageId);
                  page.open();
                  List<PagedMessage> messages = page.read(storageManager);
                  page.close();

                  int messageId = 0;

                  for (PagedMessage message : messages) {
                     message.initMessage(storageManager);
                     long[] queueIDs = message.getQueueIDs();
                     List<String> queueNames = new ArrayList<>();
                     for (long queueID : queueIDs) {
                        PagePosition posCheck = new PagePositionImpl(pageId, messageId);

                        boolean acked = false;

                        Set<PagePosition> positions = cursorRecords.get(queueID);
                        if (positions != null) {
                           acked = positions.contains(posCheck);
                        }

                        if (!acked) {
                           PersistentQueueBindingEncoding queueBinding = queueBindings.get(queueID);
                           if (queueBinding != null) {
                              SimpleString queueName = queueBinding.getQueueName();
                              queueNames.add(queueName.toString());
                           }
                        }
                     }

                     if (queueNames.size() > 0 && (message.getTransactionID() == -1 || pgTXs.contains(message.getTransactionID()))) {
                        printSingleMessageAsXML(message.getMessage(), queueNames);
                     }

                     messageId++;
                  }

                  pageId++;
               }
            }
            else {
               ActiveMQServerLogger.LOGGER.debug("Page store was null");
            }
         }
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void printSingleMessageAsXML(ServerMessage message, List<String> queues) throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGES_CHILD);
      printMessageAttributes(message);
      printMessageProperties(message);
      printMessageQueues(queues);
      printMessageBody(message);
      xmlWriter.writeEndElement(); // end MESSAGES_CHILD
      messagesPrinted++;
   }

   private void printMessageBody(ServerMessage message) throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGE_BODY);

      if (message.isLargeMessage()) {
         printLargeMessageBody((LargeServerMessage) message);
      }
      else {
         int size = message.getEndOfBodyPosition() - message.getBodyBuffer().readerIndex();
         byte[] buffer = new byte[size];
         message.getBodyBuffer().readBytes(buffer);

         xmlWriter.writeCData(encode(buffer));
      }
      xmlWriter.writeEndElement(); // end MESSAGE_BODY
   }

   private void printLargeMessageBody(LargeServerMessage message) throws XMLStreamException {
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_IS_LARGE, Boolean.TRUE.toString());
      BodyEncoder encoder = null;

      try {
         encoder = message.getBodyEncoder();
         encoder.open();
         long totalBytesWritten = 0;
         Long bufferSize;
         long bodySize = encoder.getLargeBodySize();
         for (long i = 0; i < bodySize; i += LARGE_MESSAGE_CHUNK_SIZE) {
            Long remainder = bodySize - totalBytesWritten;
            if (remainder >= LARGE_MESSAGE_CHUNK_SIZE) {
               bufferSize = LARGE_MESSAGE_CHUNK_SIZE;
            }
            else {
               bufferSize = remainder;
            }
            ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(bufferSize.intValue());
            encoder.encode(buffer, bufferSize.intValue());
            xmlWriter.writeCData(encode(buffer.toByteBuffer().array()));
            totalBytesWritten += bufferSize;
         }
         encoder.close();
      }
      catch (ActiveMQException e) {
         e.printStackTrace();
      }
      finally {
         if (encoder != null) {
            try {
               encoder.close();
            }
            catch (ActiveMQException e) {
               e.printStackTrace();
            }
         }
      }
   }

   private void printMessageQueues(List<String> queues) throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.QUEUES_PARENT);
      for (String queueName : queues) {
         xmlWriter.writeEmptyElement(XmlDataConstants.QUEUES_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.QUEUE_NAME, queueName);
      }
      xmlWriter.writeEndElement(); // end QUEUES_PARENT
   }

   private void printMessageProperties(ServerMessage message) throws XMLStreamException {
      xmlWriter.writeStartElement(XmlDataConstants.PROPERTIES_PARENT);
      for (SimpleString key : message.getPropertyNames()) {
         Object value = message.getObjectProperty(key);
         xmlWriter.writeEmptyElement(XmlDataConstants.PROPERTIES_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_NAME, key.toString());
         if (value instanceof byte[]) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_VALUE, encode((byte[]) value));
         }
         else {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_VALUE, value == null ? XmlDataConstants.NULL : value.toString());
         }

         if (value instanceof Boolean) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BOOLEAN);
         }
         else if (value instanceof Byte) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BYTE);
         }
         else if (value instanceof Short) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_SHORT);
         }
         else if (value instanceof Integer) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_INTEGER);
         }
         else if (value instanceof Long) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_LONG);
         }
         else if (value instanceof Float) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_FLOAT);
         }
         else if (value instanceof Double) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_DOUBLE);
         }
         else if (value instanceof String) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_STRING);
         }
         else if (value instanceof SimpleString) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING);
         }
         else if (value instanceof byte[]) {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BYTES);
         }
      }
      xmlWriter.writeEndElement(); // end PROPERTIES_PARENT
   }

   private void printMessageAttributes(ServerMessage message) throws XMLStreamException {
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_ID, Long.toString(message.getMessageID()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_PRIORITY, Byte.toString(message.getPriority()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_EXPIRATION, Long.toString(message.getExpiration()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_TIMESTAMP, Long.toString(message.getTimestamp()));
      byte rawType = message.getType();
      String prettyType = XmlDataConstants.DEFAULT_TYPE_PRETTY;
      if (rawType == Message.BYTES_TYPE) {
         prettyType = XmlDataConstants.BYTES_TYPE_PRETTY;
      }
      else if (rawType == Message.MAP_TYPE) {
         prettyType = XmlDataConstants.MAP_TYPE_PRETTY;
      }
      else if (rawType == Message.OBJECT_TYPE) {
         prettyType = XmlDataConstants.OBJECT_TYPE_PRETTY;
      }
      else if (rawType == Message.STREAM_TYPE) {
         prettyType = XmlDataConstants.STREAM_TYPE_PRETTY;
      }
      else if (rawType == Message.TEXT_TYPE) {
         prettyType = XmlDataConstants.TEXT_TYPE_PRETTY;
      }
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_TYPE, prettyType);
      if (message.getUserID() != null) {
         xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_USER_ID, message.getUserID().toString());
      }
   }

   private List<String> extractQueueNames(HashMap<Long, ReferenceDescribe> refMap) {
      List<String> queues = new ArrayList<>();
      for (ReferenceDescribe ref : refMap.values()) {
         queues.add(queueBindings.get(ref.refEncoding.queueID).getQueueName().toString());
      }
      return queues;
   }

   private static String encode(final byte[] data) {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Inner classes -------------------------------------------------

   /**
    * Proxy to handle indenting the XML since <code>javax.xml.stream.XMLStreamWriter</code> doesn't support that.
    */
   static class PrettyPrintHandler implements InvocationHandler {

      private final XMLStreamWriter target;

      private int depth = 0;

      private static final char INDENT_CHAR = ' ';

      private static final String LINE_SEPARATOR = System.getProperty("line.separator");

      boolean wrap = true;

      public PrettyPrintHandler(XMLStreamWriter target) {
         this.target = target;
      }

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
         String m = method.getName();

         switch (m) {
            case "writeStartElement":
               target.writeCharacters(LINE_SEPARATOR);
               target.writeCharacters(indent(depth));

               depth++;
               break;
            case "writeEndElement":
               depth--;
               if (wrap) {
                  target.writeCharacters(LINE_SEPARATOR);
                  target.writeCharacters(indent(depth));
               }
               wrap = true;
               break;
            case "writeEmptyElement":
            case "writeCData":
               target.writeCharacters(LINE_SEPARATOR);
               target.writeCharacters(indent(depth));
               break;
            case "writeCharacters":
               wrap = false;
               break;
         }

         method.invoke(target, args);

         return null;
      }

      private String indent(int depth) {
         depth *= 3; // level of indentation
         char[] output = new char[depth];
         while (depth-- > 0) {
            output[depth] = INDENT_CHAR;
         }
         return new String(output);
      }
   }
}
