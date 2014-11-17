/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.jms.persistence.impl.journal;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQBuffers;
import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.journal.Journal;
import org.apache.activemq6.core.journal.PreparedTransactionInfo;
import org.apache.activemq6.core.journal.RecordInfo;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.JournalImpl;
import org.apache.activemq6.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq6.core.replication.ReplicatedJournal;
import org.apache.activemq6.core.replication.ReplicationManager;
import org.apache.activemq6.core.server.JournalType;
import org.apache.activemq6.jms.persistence.JMSStorageManager;
import org.apache.activemq6.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq6.jms.persistence.config.PersistedDestination;
import org.apache.activemq6.jms.persistence.config.PersistedJNDI;
import org.apache.activemq6.jms.persistence.config.PersistedType;
import org.apache.activemq6.utils.IDGenerator;

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

   public static final byte JNDI_RECORD = 3;

   // Attributes ----------------------------------------------------

   private final IDGenerator idGenerator;

   private final String journalDir;

   private final boolean createDir;

   private final Journal jmsJournal;

   private volatile boolean started;

   private final Map<String, PersistedConnectionFactory> mapFactories = new ConcurrentHashMap<String, PersistedConnectionFactory>();

   private final Map<Pair<PersistedType, String>, PersistedDestination> destinations = new ConcurrentHashMap<Pair<PersistedType, String>, PersistedDestination>();

   private final Map<Pair<PersistedType, String>, PersistedJNDI> mapJNDI = new ConcurrentHashMap<Pair<PersistedType, String>, PersistedJNDI>();

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
                                         "hornetq-jms",
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

   public List<PersistedJNDI> recoverPersistedJNDI() throws Exception
   {
      ArrayList<PersistedJNDI> list = new ArrayList<PersistedJNDI>(mapJNDI.values());
      return list;
   }

   public void addJNDI(PersistedType type, String name, String... address) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      long tx = idGenerator.generateID();

      PersistedJNDI currentJNDI = mapJNDI.get(key);
      if (currentJNDI != null)
      {
         jmsJournal.appendDeleteRecordTransactional(tx, currentJNDI.getId());
      }
      else
      {
         currentJNDI = new PersistedJNDI(type, name);
      }

      mapJNDI.put(key, currentJNDI);

      for (String adItem : address)
      {
         currentJNDI.addJNDI(adItem);
      }


      long newId = idGenerator.generateID();

      currentJNDI.setId(newId);

      jmsJournal.appendAddRecordTransactional(tx, newId, JNDI_RECORD, currentJNDI);

      jmsJournal.appendCommitRecord(tx, true);
   }

   public void deleteJNDI(PersistedType type, String name, String address) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      long tx = idGenerator.generateID();

      PersistedJNDI currentJNDI = mapJNDI.get(key);
      if (currentJNDI == null)
      {
         return;
      }
      else
      {
         jmsJournal.appendDeleteRecordTransactional(tx, currentJNDI.getId());
      }

      currentJNDI.deleteJNDI(address);

      if (currentJNDI.getJndi().size() == 0)
      {
         mapJNDI.remove(key);
      }
      else
      {
         long newId = idGenerator.generateID();
         currentJNDI.setId(newId);
         jmsJournal.appendAddRecordTransactional(tx, newId, JNDI_RECORD, currentJNDI);
      }

      jmsJournal.appendCommitRecord(tx, true);
   }


   public void deleteJNDI(PersistedType type, String name) throws Exception
   {
      Pair<PersistedType, String> key = new Pair<PersistedType, String>(type, name);

      PersistedJNDI currentJNDI = mapJNDI.remove(key);

      if (currentJNDI != null)
      {
         jmsJournal.appendDeleteRecord(currentJNDI.getId(), true);
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

         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(record.data);

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
         else if (rec == JNDI_RECORD)
         {
            PersistedJNDI jndi = new PersistedJNDI();
            jndi.decode(buffer);
            jndi.setId(id);
            Pair<PersistedType, String> key = new Pair<PersistedType, String>(jndi.getType(), jndi.getName());
            mapJNDI.put(key, jndi);
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
