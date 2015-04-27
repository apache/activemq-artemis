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
package org.apache.activemq.jms.client;

import java.util.Enumeration;
import java.util.Vector;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

import org.apache.activemq.core.version.Version;

/**
 * ActiveMQ implementation of a JMS ConnectionMetaData.
 */
public class ActiveMQConnectionMetaData implements ConnectionMetaData
{
   // Constants -----------------------------------------------------

   private static final String ACTIVEMQ = "ActiveMQ";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Version serverVersion;

   // Constructors --------------------------------------------------

   /**
    * Create a new ActiveMQConnectionMetaData object.
    */
   public ActiveMQConnectionMetaData(final Version serverVersion)
   {
      this.serverVersion = serverVersion;
   }

   // ConnectionMetaData implementation -----------------------------

   public String getJMSVersion() throws JMSException
   {
      return "2.0";
   }

   public int getJMSMajorVersion() throws JMSException
   {
      return 2;
   }

   public int getJMSMinorVersion() throws JMSException
   {
      return 0;
   }

   public String getJMSProviderName() throws JMSException
   {
      return ActiveMQConnectionMetaData.ACTIVEMQ;
   }

   public String getProviderVersion() throws JMSException
   {
      return serverVersion.getFullVersion();
   }

   public int getProviderMajorVersion() throws JMSException
   {
      return serverVersion.getMajorVersion();
   }

   public int getProviderMinorVersion() throws JMSException
   {
      return serverVersion.getMinorVersion();
   }

   public Enumeration getJMSXPropertyNames() throws JMSException
   {
      Vector<Object> v = new Vector<Object>();
      v.add("JMSXGroupID");
      v.add("JMSXGroupSeq");
      v.add("JMSXDeliveryCount");
      return v.elements();
   }
}
