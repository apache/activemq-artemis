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
package org.apache.activemq.core.persistence.config;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.journal.EncodingSupport;
import org.apache.activemq.core.settings.impl.AddressSettings;

/**
 * A PersistedAddressSetting
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class PersistedAddressSetting implements EncodingSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long storeId;

   private SimpleString addressMatch;

   private AddressSettings setting;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PersistedAddressSetting()
   {
      super();
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "PersistedAddressSetting [storeId=" + storeId +
             ", addressMatch=" +
             addressMatch +
             ", setting=" +
             setting +
             "]";
   }

   /**
    * @param addressMatch
    * @param setting
    */
   public PersistedAddressSetting(SimpleString addressMatch, AddressSettings setting)
   {
      super();
      this.addressMatch = addressMatch;
      this.setting = setting;
   }

   // Public --------------------------------------------------------

   public void setStoreId(long id)
   {
      this.storeId = id;
   }

   public long getStoreId()
   {
      return storeId;
   }

   /**
    * @return the addressMatch
    */
   public SimpleString getAddressMatch()
   {
      return addressMatch;
   }

   /**
    * @return the setting
    */
   public AddressSettings getSetting()
   {
      return setting;
   }

   @Override
   public void decode(HornetQBuffer buffer)
   {
      addressMatch = buffer.readSimpleString();

      setting = new AddressSettings();
      setting.decode(buffer);
   }

   @Override
   public void encode(HornetQBuffer buffer)
   {
      buffer.writeSimpleString(addressMatch);

      setting.encode(buffer);
   }

   @Override
   public int getEncodeSize()
   {
      return addressMatch.sizeof() + setting.getEncodeSize();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
