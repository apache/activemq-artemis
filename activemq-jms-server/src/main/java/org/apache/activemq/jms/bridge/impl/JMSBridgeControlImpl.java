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
package org.apache.activemq.jms.bridge.impl;

import javax.management.StandardMBean;

import org.apache.activemq.jms.bridge.JMSBridge;
import org.apache.activemq.jms.bridge.JMSBridgeControl;
import org.apache.activemq.jms.bridge.QualityOfServiceMode;

public class JMSBridgeControlImpl extends StandardMBean implements JMSBridgeControl
{

   private final JMSBridge bridge;

   // Constructors --------------------------------------------------

   public JMSBridgeControlImpl(final JMSBridge bridge) throws Exception
   {
      super(JMSBridgeControl.class);
      this.bridge = bridge;
   }

   // Public --------------------------------------------------------

   public void pause() throws Exception
   {
      bridge.pause();
   }

   public void resume() throws Exception
   {
      bridge.resume();
   }

   public boolean isStarted()
   {
      return bridge.isStarted();
   }

   public void start() throws Exception
   {
      bridge.start();
   }

   public void stop() throws Exception
   {
      bridge.stop();
   }

   public String getClientID()
   {
      return bridge.getClientID();
   }

   public long getFailureRetryInterval()
   {
      return bridge.getFailureRetryInterval();
   }

   public int getMaxBatchSize()
   {
      return bridge.getMaxBatchSize();
   }

   public long getMaxBatchTime()
   {
      return bridge.getMaxBatchTime();
   }

   public int getMaxRetries()
   {
      return bridge.getMaxRetries();
   }

   public String getQualityOfServiceMode()
   {
      QualityOfServiceMode mode = bridge.getQualityOfServiceMode();
      if (mode != null)
      {
         return mode.name();
      }
      else
      {
         return null;
      }
   }

   public String getSelector()
   {
      return bridge.getSelector();
   }

   public String getSourcePassword()
   {
      return bridge.getSourcePassword();
   }

   public String getSourceUsername()
   {
      return bridge.getSourceUsername();
   }

   public String getSubscriptionName()
   {
      return bridge.getSubscriptionName();
   }

   public String getTargetPassword()
   {
      return bridge.getTargetPassword();
   }

   public String getTargetUsername()
   {
      return bridge.getTargetUsername();
   }

   public boolean isAddMessageIDInHeader()
   {
      return bridge.isAddMessageIDInHeader();
   }

   public boolean isFailed()
   {
      return bridge.isFailed();
   }

   public boolean isPaused()
   {
      return bridge.isPaused();
   }

   public void setAddMessageIDInHeader(final boolean value)
   {
      bridge.setAddMessageIDInHeader(value);
   }

   public void setClientID(final String clientID)
   {
      bridge.setClientID(clientID);
   }

   public void setFailureRetryInterval(final long interval)
   {
      bridge.setFailureRetryInterval(interval);
   }

   public void setMaxBatchSize(final int size)
   {
      bridge.setMaxBatchSize(size);
   }

   public void setMaxBatchTime(final long time)
   {
      bridge.setMaxBatchTime(time);
   }

   public void setMaxRetries(final int retries)
   {
      bridge.setMaxRetries(retries);
   }

   public void setQualityOfServiceMode(String mode)
   {
      if (mode != null)
      {
         bridge.setQualityOfServiceMode(QualityOfServiceMode.valueOf(mode));
      }
      else
      {
         mode = null;
      }
   }

   public void setSelector(final String selector)
   {
      bridge.setSelector(selector);
   }

   public void setSourcePassword(final String pwd)
   {
      bridge.setSourcePassword(pwd);
   }

   public void setSourceUsername(final String name)
   {
      bridge.setSourceUsername(name);
   }

   public void setSubscriptionName(final String subname)
   {
      bridge.setSubscriptionName(subname);
   }

   public void setTargetPassword(final String pwd)
   {
      bridge.setTargetPassword(pwd);
   }

   public void setTargetUsername(final String name)
   {
      bridge.setTargetUsername(name);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
