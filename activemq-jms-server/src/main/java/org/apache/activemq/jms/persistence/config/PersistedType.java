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
package org.apache.activemq.jms.persistence.config;

/**
 * A PersistedType
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public enum PersistedType
{
   ConnectionFactory, Topic, Queue;

   public byte getType()
   {
      switch (this)
      {
         case ConnectionFactory:
            return 0;
         case Topic:
            return 1;
         case Queue:
            return 2;
         default:
            return -1;
      }
   }

   public static PersistedType getType(byte type)
   {
      switch (type)
      {
         case 0:
            return ConnectionFactory;
         case 1:
            return Topic;
         case 2:
            return Queue;
         default:
            return null;
      }
   }
}
