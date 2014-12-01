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
package org.apache.activemq.jms.persistence.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.core.journal.EncodingSupport;
import org.apache.activemq.utils.BufferHelper;
import org.apache.activemq.utils.DataConstants;

/**
 * A PersistedJNDI
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class PersistedJNDI implements EncodingSupport
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long id;

   private PersistedType type;

   private String name;

   private ArrayList<String> jndi = new ArrayList<String>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PersistedJNDI()
   {
   }

   /**
    * @param type
    * @param name
    */
   public PersistedJNDI(PersistedType type, String name)
   {
      super();
      this.type = type;
      this.name = name;
   }

   // Public --------------------------------------------------------
   @Override
   public void decode(ActiveMQBuffer buffer)
   {
      type = PersistedType.getType(buffer.readByte());
      name = buffer.readSimpleString().toString();
      int jndiArraySize = buffer.readInt();
      jndi = new ArrayList<String>(jndiArraySize);

      for (int i = 0; i < jndiArraySize; i++)
      {
         jndi.add(buffer.readSimpleString().toString());
      }
   }

   @Override
   public void encode(ActiveMQBuffer buffer)
   {
      buffer.writeByte(type.getType());
      BufferHelper.writeAsSimpleString(buffer, name);
      buffer.writeInt(jndi.size());
      for (String jndiEl : jndi)
      {
         BufferHelper.writeAsSimpleString(buffer, jndiEl);
      }
   }

   @Override
   public int getEncodeSize()
   {
      return DataConstants.SIZE_BYTE +
         BufferHelper.sizeOfSimpleString(name) +
         sizeOfJNDI();
   }

   private int sizeOfJNDI()
   {
      int size = DataConstants.SIZE_INT; // for the number of elements written

      for (String str : jndi)
      {
         size += BufferHelper.sizeOfSimpleString(str);
      }

      return size;
   }

   /**
    * @return the id
    */
   public long getId()
   {
      return id;
   }

   /**
    * @param id the id to set
    */
   public void setId(long id)
   {
      this.id = id;
   }

   /**
    * @return the type
    */
   public PersistedType getType()
   {
      return type;
   }

   /**
    * @return the name
    */
   public String getName()
   {
      return name;
   }

   /**
    * @return the jndi
    */
   public List<String> getJndi()
   {
      return jndi;
   }

   public void addJNDI(String address)
   {
      jndi.add(address);
   }

   public void deleteJNDI(String address)
   {
      jndi.remove(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
