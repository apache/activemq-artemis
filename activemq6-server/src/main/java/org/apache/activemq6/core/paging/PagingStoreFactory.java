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
package org.apache.activemq6.core.paging;

import java.util.List;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.settings.HierarchicalRepository;
import org.apache.activemq6.core.settings.impl.AddressSettings;

/**
 * The integration point between the PagingManger and the File System (aka SequentialFiles)
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PagingStoreFactory
{
   PagingStore newStore(SimpleString address, AddressSettings addressSettings);

   void stop() throws InterruptedException;

   void setPagingManager(PagingManager manager);

   List<PagingStore> reloadStores(HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception;

   SequentialFileFactory newFileFactory(SimpleString address) throws Exception;

}
