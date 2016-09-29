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

import javax.jms.ConnectionMetaData;
import java.util.Enumeration;
import java.util.Vector;

/**
 * This class implements javax.jms.ConnectionMetaData
 */
public class ActiveMQRAConnectionMetaData implements ConnectionMetaData {

   /**
    * Trace enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * Constructor
    */
   public ActiveMQRAConnectionMetaData() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor()");
      }
   }

   /**
    * Get the JMS version
    *
    * @return The version
    */
   @Override
   public String getJMSVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getJMSVersion()");
      }

      return "2.0";
   }

   /**
    * Get the JMS major version
    *
    * @return The major version
    */
   @Override
   public int getJMSMajorVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getJMSMajorVersion()");
      }

      return 2;
   }

   /**
    * Get the JMS minor version
    *
    * @return The minor version
    */
   @Override
   public int getJMSMinorVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getJMSMinorVersion()");
      }

      return 0;
   }

   /**
    * Get the JMS provider name
    *
    * @return The name
    */
   @Override
   public String getJMSProviderName() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getJMSProviderName()");
      }

      return "ActiveMQ Artemis";
   }

   /**
    * Get the provider version
    *
    * @return The version
    */
   @Override
   public String getProviderVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getJMSProviderName()");
      }

      return "2.4";
   }

   /**
    * Get the provider major version
    *
    * @return The version
    */
   @Override
   public int getProviderMajorVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getProviderMajorVersion()");
      }

      return 2;
   }

   /**
    * Get the provider minor version
    *
    * @return The version
    */
   @Override
   public int getProviderMinorVersion() {
      if (ActiveMQRAConnectionMetaData.trace) {
         ActiveMQRALogger.LOGGER.trace("getProviderMinorVersion()");
      }

      return 4;
   }

   /**
    * Get the JMS XPropertyNames
    *
    * @return The names
    */
   @Override
   public Enumeration<Object> getJMSXPropertyNames() {
      Vector<Object> v = new Vector<>();
      v.add("JMSXGroupID");
      v.add("JMSXGroupSeq");
      v.add("JMSXDeliveryCount");
      return v.elements();
   }
}
