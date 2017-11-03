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
package org.apache.activemq.artemis.core.management.impl;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;

public class AcceptorControlImpl extends AbstractControl implements AcceptorControl {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Acceptor acceptor;

   private final TransportConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcceptorControlImpl(final Acceptor acceptor,
                              final StorageManager storageManager,
                              final TransportConfiguration configuration) throws Exception {
      super(AcceptorControl.class, storageManager);
      this.acceptor = acceptor;
      this.configuration = configuration;
   }

   // AcceptorControlMBean implementation ---------------------------

   @Override
   public String getFactoryClassName() {
      clearIO();
      try {
         return configuration.getFactoryClassName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public String getName() {
      clearIO();
      try {
         return configuration.getName();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public Map<String, Object> getParameters() {
      clearIO();
      try {
         Map<String, Object> clone = new HashMap(configuration.getParams());
         for (Map.Entry<String, Object> entry : clone.entrySet()) {
            if (entry.getKey().toLowerCase().contains("password")) {
               entry.setValue("****");
            }
         }
         return clone;
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void reload() {
      clearIO();
      try {
         acceptor.reload();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public boolean isStarted() {
      clearIO();
      try {
         return acceptor.isStarted();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void start() throws Exception {
      clearIO();
      try {
         acceptor.start();
      } finally {
         blockOnIO();
      }
   }

   @Override
   public void stop() throws Exception {
      clearIO();
      try {
         acceptor.stop();
      } finally {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo() {
      return MBeanInfoHelper.getMBeanOperationsInfo(AcceptorControl.class);
   }

   @Override
   protected MBeanAttributeInfo[] fillMBeanAttributeInfo() {
      return MBeanInfoHelper.getMBeanAttributesInfo(AcceptorControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
