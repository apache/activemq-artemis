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
package org.apache.activemq.artemis.core.server;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.FilterUtils;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;

public final class QueueConfig {

   private final long id;
   private final SimpleString address;
   private final SimpleString name;
   private final Filter filter;
   private final PageSubscription pageSubscription;
   private final SimpleString user;
   private final boolean durable;
   private final boolean temporary;
   private final boolean autoCreated;
   private final int maxConsumers;
   private final boolean deleteOnNoConsumers;

   public static final class Builder {

      private final long id;
      private final SimpleString address;
      private final SimpleString name;
      private Filter filter;
      private PagingManager pagingManager;
      private SimpleString user;
      private boolean durable;
      private boolean temporary;
      private boolean autoCreated;
      private int maxConsumers;
      private boolean deleteOnNoConsumers;

      private Builder(final long id, final SimpleString name) {
         this(id, name, name);
      }

      private Builder(final long id, final SimpleString name, final SimpleString address) {
         this.id = id;
         this.name = name;
         this.address = address;
         this.filter = null;
         this.pagingManager = null;
         this.user = null;
         this.durable = true;
         this.temporary = false;
         this.autoCreated = true;
         this.maxConsumers = -1;
         this.deleteOnNoConsumers = false;
         validateState();
      }

      private static boolean isEmptyOrNull(SimpleString value) {
         return (value == null || value.length() == 0);
      }

      private void validateState() {
         if (isEmptyOrNull(this.name)) {
            throw new IllegalStateException("name can't be null or empty!");
         }
         if (isEmptyOrNull(this.address)) {
            throw new IllegalStateException("address can't be null or empty!");
         }
      }

      public Builder filter(final Filter filter) {
         this.filter = filter;
         return this;
      }

      public Builder pagingManager(final PagingManager pagingManager) {
         this.pagingManager = pagingManager;
         return this;
      }

      public Builder user(final SimpleString user) {
         this.user = user;
         return this;
      }

      public Builder durable(final boolean durable) {
         this.durable = durable;
         return this;
      }

      public Builder temporary(final boolean temporary) {
         this.temporary = temporary;
         return this;
      }

      public Builder autoCreated(final boolean autoCreated) {
         this.autoCreated = autoCreated;
         return this;
      }

      public Builder maxConsumers(final int maxConsumers) {
         this.maxConsumers = maxConsumers;
         return this;
      }

      public Builder deleteOnNoConsumers(final boolean deleteOnNoConsumers) {
         this.deleteOnNoConsumers = deleteOnNoConsumers;
         return this;
      }

      /**
       * Returns a new {@link QueueConfig} using the parameters configured on the {@link Builder}.
       * <br>
       * The reference parameters aren't defensively copied from the {@link Builder} to the {@link QueueConfig}.
       * <br>
       * This method creates a new {@link PageSubscription} only if {@link #pagingManager} is not {@code null} and
       * if {@link FilterUtils#isTopicIdentification} returns {@code false} on {@link #filter}.
       *
       * @throws IllegalStateException if the creation of {@link PageSubscription} fails
       */
      public QueueConfig build() {
         final PageSubscription pageSubscription;
         if (pagingManager != null && !FilterUtils.isTopicIdentification(filter)) {
            try {
               pageSubscription = this.pagingManager.getPageStore(address).getCursorProvider().createSubscription(id, filter, durable);
            } catch (Exception e) {
               throw new IllegalStateException(e);
            }
         } else {
            pageSubscription = null;
         }
         return new QueueConfig(id, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, maxConsumers, deleteOnNoConsumers);
      }

   }

   /**
    * Returns a new {@link Builder} of a durable, not temporary and autoCreated {@link QueueConfig} with the given {@code id} and {@code name}.
    * <br>
    * The {@code address} is defaulted to the {@code name} value.
    * The reference parameters aren't defensively copied.
    *
    * @param id   the id of the queue to be created
    * @param name the name of the queue to be created
    * @throws IllegalStateException if {@code name} is {@code null} or empty
    */
   public static Builder builderWith(final long id, final SimpleString name) {
      return new QueueConfig.Builder(id, name);
   }

   /**
    * Returns a new {@link Builder} of a durable, not temporary and autoCreated {@link QueueConfig} with the given {@code id}, {@code name} and {@code address}.
    * <br>
    * The reference parameters aren't defensively copied.
    *
    * @param id      the id of the queue to be created
    * @param name    the name of the queue to be created
    * @param address the address of the queue to be created
    * @throws IllegalStateException if {@code name} or {@code address} are {@code null} or empty
    */
   public static Builder builderWith(final long id, final SimpleString name, final SimpleString address) {
      return new QueueConfig.Builder(id, name, address);
   }

   private QueueConfig(final long id,
                       final SimpleString address,
                       final SimpleString name,
                       final Filter filter,
                       final PageSubscription pageSubscription,
                       final SimpleString user,
                       final boolean durable,
                       final boolean temporary,
                       final boolean autoCreated,
                       final int maxConsumers,
                       final boolean deleteOnNoConsumers) {
      this.id = id;
      this.address = address;
      this.name = name;
      this.filter = filter;
      this.pageSubscription = pageSubscription;
      this.user = user;
      this.durable = durable;
      this.temporary = temporary;
      this.autoCreated = autoCreated;
      this.deleteOnNoConsumers = deleteOnNoConsumers;
      this.maxConsumers = maxConsumers;
   }

   public long id() {
      return id;
   }

   public SimpleString address() {
      return address;
   }

   public SimpleString name() {
      return name;
   }

   public Filter filter() {
      return filter;
   }

   public PageSubscription pageSubscription() {
      return pageSubscription;
   }

   public SimpleString user() {
      return user;
   }

   public boolean isDurable() {
      return durable;
   }

   public boolean isTemporary() {
      return temporary;
   }

   public boolean isAutoCreated() {
      return autoCreated;
   }

   public boolean isDeleteOnNoConsumers() {
      return deleteOnNoConsumers;
   }

   public int maxConsumers() {
      return maxConsumers;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      QueueConfig that = (QueueConfig) o;

      if (id != that.id)
         return false;
      if (durable != that.durable)
         return false;
      if (temporary != that.temporary)
         return false;
      if (autoCreated != that.autoCreated)
         return false;
      if (address != null ? !address.equals(that.address) : that.address != null)
         return false;
      if (name != null ? !name.equals(that.name) : that.name != null)
         return false;
      if (filter != null ? !filter.equals(that.filter) : that.filter != null)
         return false;
      if (pageSubscription != null ? !pageSubscription.equals(that.pageSubscription) : that.pageSubscription != null)
         return false;
      if (maxConsumers != that.maxConsumers)
         return false;
      if (deleteOnNoConsumers != that.deleteOnNoConsumers)
         return false;
      return user != null ? user.equals(that.user) : that.user == null;

   }

   @Override
   public int hashCode() {
      int result = (int) (id ^ (id >>> 32));
      result = 31 * result + (address != null ? address.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (filter != null ? filter.hashCode() : 0);
      result = 31 * result + (pageSubscription != null ? pageSubscription.hashCode() : 0);
      result = 31 * result + (user != null ? user.hashCode() : 0);
      result = 31 * result + (durable ? 1 : 0);
      result = 31 * result + (temporary ? 1 : 0);
      result = 31 * result + (autoCreated ? 1 : 0);
      result = 31 * result + maxConsumers;
      result = 31 * result + (deleteOnNoConsumers ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "QueueConfig{"
         + "id=" + id
         + ", address=" + address
         + ", name=" + name
         + ", filter=" + filter
         + ", pageSubscription=" + pageSubscription
         + ", user=" + user
         + ", durable=" + durable
         + ", temporary=" + temporary
         + ", autoCreated=" + autoCreated
         + ", maxConsumers=" + maxConsumers
         + ", deleteOnNoConsumers=" + deleteOnNoConsumers + '}';
   }
}
