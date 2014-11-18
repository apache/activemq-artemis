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
package org.apache.activemq.ra;

import java.util.Enumeration;
import java.util.Vector;

import javax.jms.ConnectionMetaData;


/**
 * This class implements javax.jms.ConnectionMetaData
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class ActiveMQRAConnectionMetaData implements ConnectionMetaData
{
   /** Trace enabled */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * Constructor
    */
   public ActiveMQRAConnectionMetaData()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("constructor()");
      }
   }

   /**
    * Get the JMS version
    * @return The version
    */
   public String getJMSVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getJMSVersion()");
      }

      return "2.0";
   }

   /**
    * Get the JMS major version
    * @return The major version
    */
   public int getJMSMajorVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getJMSMajorVersion()");
      }

      return 2;
   }

   /**
    * Get the JMS minor version
    * @return The minor version
    */
   public int getJMSMinorVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getJMSMinorVersion()");
      }

      return 0;
   }

   /**
    * Get the JMS provider name
    * @return The name
    */
   public String getJMSProviderName()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getJMSProviderName()");
      }

      return "ActiveMQ";
   }

   /**
    * Get the provider version
    * @return The version
    */
   public String getProviderVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getJMSProviderName()");
      }

      return "2.4";
   }

   /**
    * Get the provider major version
    * @return The version
    */
   public int getProviderMajorVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getProviderMajorVersion()");
      }

      return 2;
   }

   /**
    * Get the provider minor version
    * @return The version
    */
   public int getProviderMinorVersion()
   {
      if (ActiveMQRAConnectionMetaData.trace)
      {
         ActiveMQRALogger.LOGGER.trace("getProviderMinorVersion()");
      }

      return 4;
   }

   /**
    * Get the JMS XPropertyNames
    * @return The names
    */
   public Enumeration<Object> getJMSXPropertyNames()
   {
      Vector<Object> v = new Vector<Object>();
      v.add("JMSXGroupID");
      v.add("JMSXGroupSeq");
      v.add("JMSXDeliveryCount");
      return v.elements();
   }
}
