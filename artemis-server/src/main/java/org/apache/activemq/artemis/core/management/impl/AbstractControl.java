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
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.activemq.artemis.core.persistence.StorageManager;

public abstract class AbstractControl extends StandardMBean {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected final StorageManager storageManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AbstractControl(final Class<?> clazz, final StorageManager storageManager) throws NotCompliantMBeanException {
      super(clazz);
      this.storageManager = storageManager;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void clearIO() {
      // the storage manager could be null on the backup on certain components
      if (storageManager != null) {
         storageManager.clearContext();
      }
   }

   protected void blockOnIO() {
      // the storage manager could be null on the backup on certain components
      if (storageManager != null) {
         try {
            storageManager.waitOnOperations();
            storageManager.clearContext();
         } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

   }

   protected abstract MBeanOperationInfo[] fillMBeanOperationInfo();

   protected abstract MBeanAttributeInfo[] fillMBeanAttributeInfo();

   @Override
   public MBeanInfo getMBeanInfo() {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), fillMBeanAttributeInfo(), info.getConstructors(), fillMBeanOperationInfo(), info.getNotifications());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
