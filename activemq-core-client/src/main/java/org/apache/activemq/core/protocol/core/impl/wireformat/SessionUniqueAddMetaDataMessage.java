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
package org.apache.activemq.core.protocol.core.impl.wireformat;


/**
 * A SessionUniqueAddMetaDataMessageV2
 *
 * @author clebertsuconic
 *
 *
 */
public class SessionUniqueAddMetaDataMessage extends SessionAddMetaDataMessageV2
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionUniqueAddMetaDataMessage()
   {
      super(SESS_UNIQUE_ADD_METADATA);
   }


   public SessionUniqueAddMetaDataMessage(String key, String data)
   {
      super(SESS_UNIQUE_ADD_METADATA, key, data);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
