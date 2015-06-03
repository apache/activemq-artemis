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
package org.apache.activemq.artemis.ra;

import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnectionMetaData;


/**
 * Managed connection meta data
 */
public class ActiveMQRAMetaData implements ManagedConnectionMetaData
{
   /** Trace enabled */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /** The managed connection */
   private final ActiveMQRAManagedConnection mc;

   /**
    * Constructor
    * @param mc The managed connection
    */
   public ActiveMQRAMetaData(final ActiveMQRAManagedConnection mc)
   {
      if (ActiveMQRAMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("constructor(" + mc + ")");
      }

      this.mc = mc;
   }

   /**
    * Get the EIS product name
    * @return The name
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductName() throws ResourceException
   {
      if (ActiveMQRAMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getEISProductName()");
      }

      return "ActiveMQ Artemis";
   }

   /**
    * Get the EIS product version
    * @return The version
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductVersion() throws ResourceException
   {
      if (ActiveMQRAMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getEISProductVersion()");
      }

      return "2.0";
   }

   /**
    * Get the user name
    * @return The user name
    * @exception ResourceException Thrown if operation fails
    */
   public String getUserName() throws ResourceException
   {
      if (ActiveMQRAMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getUserName()");
      }

      return mc.getUserName();
   }

   /**
     * Get the maximum number of connections -- RETURNS 0
     * @return The number
     * @exception ResourceException Thrown if operation fails
     */
   public int getMaxConnections() throws ResourceException
   {
      if (ActiveMQRAMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getMaxConnections()");
      }

      return 0;
   }

}
