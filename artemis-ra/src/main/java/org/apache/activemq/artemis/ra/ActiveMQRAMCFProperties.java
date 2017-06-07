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

import javax.jms.Queue;
import javax.jms.Topic;
import java.io.Serializable;

/**
 * The MCF default properties - these are set in the tx-connection-factory at the jms-ds.xml
 */
public class ActiveMQRAMCFProperties extends ConnectionFactoryProperties implements Serializable {

   /**
    * Serial version UID
    */
   static final long serialVersionUID = -5951352236582886862L;
   /**
    * Trace enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   /**
    * The queue type
    */
   private static final String QUEUE_TYPE = Queue.class.getName();

   /**
    * The topic type
    */
   private static final String TOPIC_TYPE = Topic.class.getName();
   protected boolean allowLocalTransactions;

   private String strConnectorClassName;

   public String strConnectionParameters;

   /**
    * The connection type
    */
   private int type = ActiveMQRAConnectionFactory.CONNECTION;

   /**
    * Use tryLock
    */
   private Integer useTryLock;

   /**
    * Constructor
    */
   public ActiveMQRAMCFProperties() {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor()");
      }

      useTryLock = null;
   }

   /**
    * Get the connection type
    *
    * @return The type
    */
   public int getType() {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("getType()");
      }

      return type;
   }

   public String getConnectorClassName() {
      return strConnectorClassName;
   }

   public void setConnectorClassName(final String connectorClassName) {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("setConnectorClassName(" + connectorClassName + ")");
      }

      strConnectorClassName = connectorClassName;

      setParsedConnectorClassNames(ActiveMQRaUtils.parseConnectorConnectorConfig(connectorClassName));
   }

   /**
    * @return the connectionParameters
    */
   public String getStrConnectionParameters() {
      return strConnectionParameters;
   }

   public void setConnectionParameters(final String configuration) {
      strConnectionParameters = configuration;
      setParsedConnectionParameters(ActiveMQRaUtils.parseConfig(configuration));
   }

   /**
    * Set the default session type.
    *
    * @param defaultType either javax.jms.Topic or javax.jms.Queue
    */
   public void setSessionDefaultType(final String defaultType) {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("setSessionDefaultType(" + type + ")");
      }

      if (defaultType.equals(ActiveMQRAMCFProperties.QUEUE_TYPE)) {
         type = ActiveMQRAConnectionFactory.QUEUE_CONNECTION;
      } else if (defaultType.equals(ActiveMQRAMCFProperties.TOPIC_TYPE)) {
         type = ActiveMQRAConnectionFactory.TOPIC_CONNECTION;
      } else {
         type = ActiveMQRAConnectionFactory.CONNECTION;
      }
   }

   /**
    * Get the default session type.
    *
    * @return The default session type
    */
   public String getSessionDefaultType() {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("getSessionDefaultType()");
      }

      if (type == ActiveMQRAConnectionFactory.CONNECTION) {
         return "BOTH";
      } else if (type == ActiveMQRAConnectionFactory.QUEUE_CONNECTION) {
         return ActiveMQRAMCFProperties.TOPIC_TYPE;
      } else {
         return ActiveMQRAMCFProperties.QUEUE_TYPE;
      }
   }

   /**
    * Get the useTryLock.
    *
    * @return the useTryLock.
    */
   public Integer getUseTryLock() {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("getUseTryLock()");
      }

      return useTryLock;
   }

   /**
    * Set the useTryLock.
    *
    * @param useTryLock the useTryLock.
    */
   public void setUseTryLock(final Integer useTryLock) {
      if (ActiveMQRAMCFProperties.trace) {
         ActiveMQRALogger.LOGGER.trace("setUseTryLock(" + useTryLock + ")");
      }

      this.useTryLock = useTryLock;
   }

   public boolean isAllowLocalTransactions() {
      return allowLocalTransactions;
   }

   public void setAllowLocalTransactions(boolean allowLocalTransactions) {
      this.allowLocalTransactions = allowLocalTransactions;
   }
}
