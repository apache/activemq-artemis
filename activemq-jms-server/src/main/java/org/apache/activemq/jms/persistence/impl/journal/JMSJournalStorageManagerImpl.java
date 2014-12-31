/**
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
package org.apache.activemq.jms.persistence.impl.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.journal.Journal;
import org.apache.activemq.core.journal.PreparedTransactionInfo;
import org.apache.activemq.core.journal.RecordInfo;
import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.core.journal.impl.JournalImpl;
import org.apache.activemq.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq.core.replication.ReplicatedJournal;
import org.apache.activemq.core.replication.ReplicationManager;
import org.apache.activemq.core.server.JournalType;
import org.apache.activemq.jms.persistence.JMSStorageManager;
import org.apache.activemq.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq.jms.persistence.config.PersistedDestination;
import org.apache.activemq.jms.persistence.config.PersistedBindings;
import org.apache.activemq.jms.persistence.config.PersistedType;
import org.apache.activemq.utils.IDGenerator;

/**
 * A JournalJMSStorageManagerImpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public final class JMSJournalStorageManagerImpl implements JMSStorageManager
{

   // Constants -----------------------------------------------------

   public static final byte CF_RECORD = 1;

   public static final byte DESTINATION_RECORD = 2;

   public static final byte BINDING_RECORD = 3;

   // Attributes ----------------------------------------------------

   private final IDGenerator idGenerator;

   private final String journalDir;

   private final boolean createDir;

   private final Journal jmsJournal;

   private volatile boolean started;

   private final Map<String, PersistedConnectionFactory> mapFactories = new ConcurrentHashMap<String, PersistedConnectionFactory>();

   private final Map<Pair<PersistedType, String>, PersistedDestination> destinations = new ConcurrentHashMap<Pair<PersistedType, String>, PersistedDestination>();

   private final Map<Pair<PersistedType, String>, PersistedBindings> mapBindings = new ConcurrentHashMap<Pair<PersistedType, String>, PersistedBindings>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public JMSJournalStorageManagerImpl(final IDGenerator idGenerator,
                                       final Configuration config,
                                       final ReplicationManager replicator)
   {
      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw new IllegalArgumentException("Only NIO and AsyncIO are supported journals");
      }

      // Will use the same place as the bindings directory from the core journal
      journalDir = config.getBindingsDirectory();

      if (journalDir == null)
      {
         throw new NullPointerException("bindings-dir is null");
      }

      createDir = config.isCreateBindingsDir();

      SequentialFileFactory bindingsJMS = new NIOSequentialFileFactory(journalDir);

      Journal localJMS = new JournalImpl(1024 * 1024,
                                         2,
                                         config.getJournalCompactMinFiles(),
                                         config.getJournalCompactPercentage(),
                                         bindingsJMS,
                                         "activemq-jms",
                                         "jms",
                                         1);

      if (replicator != null)
      {
         jmsJournal = new ReplicatedJournal((byte) 2, localJMS, replicator);
      }
      else
      {
         jmsJournal = localJMS;
      }

      this.idGenerator = idGenerator;
   }


   // Public --------------------------------------------------------
   @Override
   public List<PersistedConnectionFactory> recoverConnectionFactories()
   {
      List<PersistedConnectionFactory> cfs = new ArrayList<PersistedConnectionFactory>(mapFactories.values());
      return cfs;
   }

   @Override
   public void storeConnectionFactory(final PersistedConnectionFactory connectionFactory) throws Exception
   {
      deleteConnectionFactory(connectionFactory.getName());
      long id = idGenerator.generateID();
      connectionFactory.setId(id);
      jmsJournal.appendAddRecord(id, CF_RECORD, connectionFactory, true);
      mapFactories.put(connectionFactory.getName(), connectionFactory);
   }

   public void deleteConnectionFactory(final String cfName) throws Exception
   {
      PersistedConnectionFactory oldCF = mapFactories.remove(cfName);
      if (oldCF != null)
      {
         jmsJournal.appendDeleteRecord(oldCF.getId(), false);
      }
   }

   @Override
   public List<PersistedDestination> recoverDestinations()
   {
      List<PersistedDestination> destinations = new ArrayList<PersistedDestination>(this.destinations.values());
      return destinations;
   }

   @Override
   public void storeDestination(final PersistedDestination destination) throws Exception
   {
      deleteDestination(destination.getType(), destination.getName());
      long id = idGenerator.generateID();
      destination.setId(id);
      jmsJournal.appendAddRecord(id, DESTINATION_RECORD, destination, true);
      destinations.put(new Pair<PersistedType, String>(destination.getType(), destination.getName()), destination);
   }

   public List<PersistedBindings> recoverPersistedBindings() throws Exception
   {
      ArrayList<PersistedBindings> list = new ArrayList<PersistedBindings>(mapBindings.values());
      return list;
   }

   public void addBindings(PersistedType type, String name, String... address) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      long tx = idGenerator.generateID();

      PersistedBindings currentBindings = mapBindings.get(key);
      if (currentBindings != null)
      {
         jmsJournal.appendDeleteRecordTransactional(tx, currentBindings.getId());
      }
      else
      {
         currentBindings = new PersistedBindings(type, name);
      }

      mapBindings.put(key, currentBindings);

      for (String adItem : address)
      {
         currentBindings.addBinding(adItem);
      }


      long newId = idGenerator.generateID();

      currentBindings.setId(newId);

      jmsJournal.appendAddRecordTransactional(tx, newId, BINDING_RECORD, currentBindings);

      jmsJournal.appendCommitRecord(tx, true);
   }

   public void deleteBindings(PersistedType type, String name, String address) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      long tx = idGenerator.generateID();

      PersistedBindings currentBindings = mapBindings.get(key);
      if (currentBindings == null)
      {
         return;
      }
      else
      {
         jmsJournal.appendDeleteRecordTransactional(tx, currentBindings.getId());
      }

      currentBindings.deleteBinding(address);

      if (currentBindings.getBindings().size() == 0)
      {
         mapBindings.remove(key);
      }
      else
      {
         long newId = idGenerator.generateID();
         currentBindings.setId(newId);
         jmsJournal.appendAddRecordTransactional(tx, newId, BINDING_RECORD, currentBindings);
      }

      jmsJournal.appendCommitRecord(tx, true);
   }


   public void deleteBindings(PersistedType type, String name) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      PersistedBindings currentBindings = mapBindings.remove(key);

      if (currentBindings != null)
      {
         jmsJournal.appendDeleteRecord(currentBindings.getId(), true);
      }
   }

   public void deleteDestination(final PersistedType type, final String name) throws Exception
   {
      PersistedDestination destination = destinations.remove(new Pair<PersistedType, String>(type, name));
      if (destination != null)
      {
         jmsJournal.appendDeleteRecord(destination.getId(), false);
      }
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }


   @Override
   public void start() throws Exception
   {

      checkAndCreateDir(journalDir, createDir);

      jmsJournal.start();

      started = true;
   }

   @Override
   public void stop() throws Exception
   {
      this.started = false;
      jmsJournal.stop();
   }

   public void load() throws Exception
   {
      mapFactories.clear();

      List<RecordInfo> data = new ArrayList<RecordInfo>();

      ArrayList<PreparedTransactionInfo> list = new ArrayList<PreparedTransactionInfo>();

      jmsJournal.load(data, list, null);

      for (RecordInfo record : data)
      {
         long id = record.id;

         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();

         if (rec == CF_RECORD)
         {
            PersistedConnectionFactory cf = new PersistedConnectionFactory();
            cf.decode(buffer);
            cf.setId(id);
            mapFactories.put(cf.getName(), cf);
         }
         else if (rec == DESTINATION_RECORD)
         {
            PersistedDestination destination = new PersistedDestination();
            destination.decode(buffer);
            destination.setId(id);
            destinations.put(new Pair<PersistedType, String>(destination.getType(), destination.getName()), destination);
         }
         else if (rec == BINDING_RECORD)
         {
            PersistedBindings bindings = new PersistedBindings();
            bindings.decode(buffer);
            bindings.setId(id);
            Pair<PersistedType, String> key = new Pair<PersistedType, String>(bindings.getType(), bindings.getName());
            mapBindings.put(key, bindings);
         }
         else
         {
            throw new IllegalStateException("Invalid record type " + rec);
         }

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------


   private void checkAndCreateDir(final String dir, final boolean create)
   {
      File f = new File(dir);

      if (!f.exists())
      {
         if (create)
         {
            if (!f.mkdirs())
            {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         }
         else
         {
            throw new IllegalArgumentException("Directory " + dir + " does not exist and will not create it");
         }
      }
   }


   // Inner classes -------------------------------------------------

}
