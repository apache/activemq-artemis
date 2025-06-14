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
package org.apache.activemq.artemis.core.persistence.config;

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;

import static org.apache.activemq.artemis.utils.DataConstants.SIZE_INT;
import static org.apache.activemq.artemis.utils.DataConstants.SIZE_NULL;

public class PersistedSecuritySetting implements EncodingSupport {

   private long storeId;

   private SimpleString addressMatch;

   private SimpleString sendRoles;

   private SimpleString consumeRoles;

   private SimpleString createDurableQueueRoles;

   private SimpleString deleteDurableQueueRoles;

   private SimpleString createNonDurableQueueRoles;

   private SimpleString deleteNonDurableQueueRoles;

   private SimpleString manageRoles;

   private SimpleString browseRoles;

   private SimpleString createAddressRoles;

   private SimpleString deleteAddressRoles;

   private SimpleString viewRoles;

   private SimpleString editRoles;


   public PersistedSecuritySetting() {
   }

   public PersistedSecuritySetting(final String addressMatch,
                                   final String sendRoles,
                                   final String consumeRoles,
                                   final String createDurableQueueRoles,
                                   final String deleteDurableQueueRoles,
                                   final String createNonDurableQueueRoles,
                                   final String deleteNonDurableQueueRoles,
                                   final String manageRoles,
                                   final String browseRoles,
                                   final String createAddressRoles,
                                   final String deleteAddressRoles,
                                   final String viewRoles,
                                   final String editRoles) {
      super();
      this.addressMatch = SimpleString.of(addressMatch);
      this.sendRoles = SimpleString.of(sendRoles);
      this.consumeRoles = SimpleString.of(consumeRoles);
      this.createDurableQueueRoles = SimpleString.of(createDurableQueueRoles);
      this.deleteDurableQueueRoles = SimpleString.of(deleteDurableQueueRoles);
      this.createNonDurableQueueRoles = SimpleString.of(createNonDurableQueueRoles);
      this.deleteNonDurableQueueRoles = SimpleString.of(deleteNonDurableQueueRoles);
      this.manageRoles = SimpleString.of(manageRoles);
      this.browseRoles = SimpleString.of(browseRoles);
      this.createAddressRoles = SimpleString.of(createAddressRoles);
      this.deleteAddressRoles = SimpleString.of(deleteAddressRoles);
      this.viewRoles = SimpleString.of(viewRoles);
      this.editRoles = SimpleString.of(editRoles);
   }


   public long getStoreId() {
      return storeId;
   }

   public void setStoreId(final long id) {
      storeId = id;
   }

   public SimpleString getAddressMatch() {
      return addressMatch;
   }

   public String getSendRoles() {
      return stringFrom(sendRoles);
   }

   public String getConsumeRoles() {
      return stringFrom(consumeRoles);
   }

   public String getCreateDurableQueueRoles() {
      return stringFrom(createDurableQueueRoles);
   }

   public String getDeleteDurableQueueRoles() {
      return stringFrom(deleteDurableQueueRoles);
   }

   public String getCreateNonDurableQueueRoles() {
      return stringFrom(createNonDurableQueueRoles);
   }

   public String getDeleteNonDurableQueueRoles() {
      return stringFrom(deleteNonDurableQueueRoles);
   }

   public String getManageRoles() {
      return stringFrom(manageRoles);
   }

   public String getBrowseRoles() {
      return stringFrom(browseRoles);
   }

   public String getCreateAddressRoles() {
      return stringFrom(createAddressRoles);
   }

   public String getDeleteAddressRoles() {
      return stringFrom(deleteAddressRoles);
   }

   public String getViewRoles() {
      return stringFrom(viewRoles);
   }

   public String getEditRoles() {
      return stringFrom(editRoles);
   }

   private String stringFrom(SimpleString possiblyNullSimpleString) {
      return possiblyNullSimpleString != null ? possiblyNullSimpleString.toString() : null;
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      if (addressMatch == null) {
         addressMatch = SimpleString.of("");
      }
      buffer.writeSimpleString(addressMatch);
      buffer.writeNullableSimpleString(sendRoles);
      buffer.writeNullableSimpleString(consumeRoles);
      buffer.writeNullableSimpleString(createDurableQueueRoles);
      buffer.writeNullableSimpleString(deleteDurableQueueRoles);
      buffer.writeNullableSimpleString(createNonDurableQueueRoles);
      buffer.writeNullableSimpleString(deleteNonDurableQueueRoles);
      buffer.writeNullableSimpleString(manageRoles);
      buffer.writeNullableSimpleString(browseRoles);
      buffer.writeNullableSimpleString(createAddressRoles);
      buffer.writeNullableSimpleString(deleteAddressRoles);
      buffer.writeNullableSimpleString(viewRoles);
      buffer.writeNullableSimpleString(editRoles);
   }

   @Override
   public int getEncodeSize() {
      return
         (addressMatch == null ? SIZE_INT : addressMatch.sizeof()) +
         (sendRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(sendRoles)) +
         (consumeRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(consumeRoles)) +
         (createDurableQueueRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(createDurableQueueRoles)) +
         (deleteDurableQueueRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(deleteDurableQueueRoles)) +
         (createNonDurableQueueRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(createNonDurableQueueRoles)) +
         (deleteNonDurableQueueRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(deleteNonDurableQueueRoles)) +
         (manageRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(manageRoles)) +
         (browseRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(browseRoles)) +
         (createAddressRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(createAddressRoles)) +
         (deleteAddressRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(deleteAddressRoles)) +
         (viewRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(viewRoles)) +
         (editRoles == null ? SIZE_NULL : SimpleString.sizeofNullableString(editRoles));
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      addressMatch = buffer.readSimpleString();
      sendRoles = buffer.readNullableSimpleString();
      consumeRoles = buffer.readNullableSimpleString();
      createDurableQueueRoles = buffer.readNullableSimpleString();
      deleteDurableQueueRoles = buffer.readNullableSimpleString();
      createNonDurableQueueRoles = buffer.readNullableSimpleString();
      deleteNonDurableQueueRoles = buffer.readNullableSimpleString();
      manageRoles = buffer.readNullableSimpleString();
      browseRoles = buffer.readNullableSimpleString();
      createAddressRoles = buffer.readNullableSimpleString();
      deleteAddressRoles = buffer.readNullableSimpleString();
      if (buffer.readableBytes() > 0) {
         viewRoles = buffer.readNullableSimpleString();
      }
      if (buffer.readableBytes() > 0) {
         editRoles = buffer.readNullableSimpleString();
      }
   }

   @Override
   public int hashCode() {
      return Objects.hash(addressMatch, consumeRoles, createDurableQueueRoles, createNonDurableQueueRoles,
                          deleteDurableQueueRoles, deleteNonDurableQueueRoles, manageRoles, browseRoles,
                          createAddressRoles, deleteAddressRoles, sendRoles, viewRoles, editRoles, storeId);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PersistedSecuritySetting other = (PersistedSecuritySetting) obj;
      if (addressMatch == null) {
         if (other.addressMatch != null)
            return false;
      } else if (!addressMatch.equals(other.addressMatch))
         return false;
      if (consumeRoles == null) {
         if (other.consumeRoles != null)
            return false;
      } else if (!consumeRoles.equals(other.consumeRoles))
         return false;
      if (createDurableQueueRoles == null) {
         if (other.createDurableQueueRoles != null)
            return false;
      } else if (!createDurableQueueRoles.equals(other.createDurableQueueRoles))
         return false;
      if (createNonDurableQueueRoles == null) {
         if (other.createNonDurableQueueRoles != null)
            return false;
      } else if (!createNonDurableQueueRoles.equals(other.createNonDurableQueueRoles))
         return false;
      if (deleteDurableQueueRoles == null) {
         if (other.deleteDurableQueueRoles != null)
            return false;
      } else if (!deleteDurableQueueRoles.equals(other.deleteDurableQueueRoles))
         return false;
      if (deleteNonDurableQueueRoles == null) {
         if (other.deleteNonDurableQueueRoles != null)
            return false;
      } else if (!deleteNonDurableQueueRoles.equals(other.deleteNonDurableQueueRoles))
         return false;
      if (manageRoles == null) {
         if (other.manageRoles != null)
            return false;
      } else if (!manageRoles.equals(other.manageRoles))
         return false;
      if (browseRoles == null) {
         if (other.browseRoles != null)
            return false;
      } else if (!browseRoles.equals(other.browseRoles))
         return false;
      if (createAddressRoles == null) {
         if (other.createAddressRoles != null)
            return false;
      } else if (!createAddressRoles.equals(other.createAddressRoles))
         return false;
      if (deleteAddressRoles == null) {
         if (other.deleteAddressRoles != null)
            return false;
      } else if (!deleteAddressRoles.equals(other.deleteAddressRoles))
         return false;
      if (sendRoles == null) {
         if (other.sendRoles != null)
            return false;
      } else if (!sendRoles.equals(other.sendRoles))
         return false;
      if (viewRoles == null) {
         if (other.viewRoles != null)
            return false;
      } else if (!viewRoles.equals(other.viewRoles))
         return false;
      if (editRoles == null) {
         if (other.editRoles != null)
            return false;
      } else if (!editRoles.equals(other.editRoles))
         return false;
      if (storeId != other.storeId)
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "PersistedSecuritySetting [storeId=" + storeId +
         ", addressMatch=" +
         addressMatch +
         ", sendRoles=" +
         sendRoles +
         ", consumeRoles=" +
         consumeRoles +
         ", createDurableQueueRoles=" +
         createDurableQueueRoles +
         ", deleteDurableQueueRoles=" +
         deleteDurableQueueRoles +
         ", createNonDurableQueueRoles=" +
         createNonDurableQueueRoles +
         ", deleteNonDurableQueueRoles=" +
         deleteNonDurableQueueRoles +
         ", manageRoles=" +
         manageRoles +
         ", browseRoles=" +
         browseRoles +
         ", createAddressRoles=" +
         createAddressRoles +
         ", deleteAddressRoles=" +
         deleteAddressRoles +
         ", viewRoles=" +
         viewRoles +
         ", editRoles=" + editRoles +
         "]";
   }

}
