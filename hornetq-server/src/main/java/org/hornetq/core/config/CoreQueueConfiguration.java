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
package org.hornetq.core.config;

import java.io.Serializable;

/**
 * A QueueConfiguration
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class CoreQueueConfiguration implements Serializable
{
   private static final long serialVersionUID = 650404974977490254L;

   private String address;

   private String name;

   private String filterString;

   private boolean durable;

   public CoreQueueConfiguration(final String address, final String name, final String filterString, final boolean durable)
   {
      this.address = address;
      this.name = name;
      this.filterString = filterString;
      this.durable = durable;
   }

   public String getAddress()
   {
      return address;
   }

   public String getName()
   {
      return name;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public boolean isDurable()
   {
      return durable;
   }

   /**
    * @param address the address to set
    */
   public void setAddress(final String address)
   {
      this.address = address;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(final String filterString)
   {
      this.filterString = filterString;
   }

   /**
    * @param durable the durable to set
    */
   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (durable ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CoreQueueConfiguration other = (CoreQueueConfiguration)obj;
      if (address == null)
      {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      if (durable != other.durable)
         return false;
      if (filterString == null)
      {
         if (other.filterString != null)
            return false;
      }
      else if (!filterString.equals(other.filterString))
         return false;
      if (name == null)
      {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      return true;
   }
}
