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
package org.apache.activemq6.jms.persistence.impl.nullpm;

import java.util.Collections;
import java.util.List;

import org.apache.activemq6.jms.persistence.JMSStorageManager;
import org.apache.activemq6.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq6.jms.persistence.config.PersistedDestination;
import org.apache.activemq6.jms.persistence.config.PersistedJNDI;
import org.apache.activemq6.jms.persistence.config.PersistedType;

/**
 * A NullJMSStorageManagerImpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class NullJMSStorageManagerImpl implements JMSStorageManager
{

   @Override
   public void deleteConnectionFactory(String connectionFactory) throws Exception
   {

   }

   @Override
   public List<PersistedConnectionFactory> recoverConnectionFactories()
   {
      return Collections.emptyList();
   }

   @Override
   public List<PersistedDestination> recoverDestinations()
   {
      return Collections.emptyList();
   }

   @Override
   public void storeConnectionFactory(PersistedConnectionFactory connectionFactory) throws Exception
   {
   }

   @Override
   public void storeDestination(PersistedDestination destination)
   {
   }

   @Override
   public boolean isStarted()
   {
      return true;
   }

   @Override
   public void start() throws Exception
   {
   }

   @Override
   public void stop() throws Exception
   {
   }

   @Override
   public void addJNDI(PersistedType type, String name, String ... address) throws Exception
   {
   }

   @Override
   public void deleteJNDI(PersistedType type, String name, String address) throws Exception
   {
   }

   @Override
   public void deleteDestination(PersistedType type, String name) throws Exception
   {
   }

   @Override
   public void deleteJNDI(PersistedType type, String name) throws Exception
   {
   }

   @Override
   public List<PersistedJNDI> recoverPersistedJNDI() throws Exception
   {
      return Collections.emptyList();
   }

   @Override
   public void load() throws Exception
   {
   }
}
