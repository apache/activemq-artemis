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
package org.apache.activemq6.core.management.impl;

import javax.management.MBeanOperationInfo;

import org.apache.activemq6.api.core.management.DivertControl;
import org.apache.activemq6.core.config.DivertConfiguration;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.server.Divert;

/**
 * A DivertControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:09:04
 */
public class DivertControlImpl extends AbstractControl implements DivertControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Divert divert;

   private final DivertConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlMBean implementation ---------------------------

   public DivertControlImpl(final Divert divert,
                            final StorageManager storageManager,
                            final DivertConfiguration configuration) throws Exception
   {
      super(DivertControl.class, storageManager);
      this.divert = divert;
      this.configuration = configuration;
   }

   public String getAddress()
   {
      clearIO();
      try
      {
         return configuration.getAddress();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getFilter()
   {
      clearIO();
      try
      {
         return configuration.getFilterString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getForwardingAddress()
   {
      clearIO();
      try
      {
         return configuration.getForwardingAddress();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getRoutingName()
   {
      clearIO();
      try
      {
         return divert.getRoutingName().toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getTransformerClassName()
   {
      clearIO();
      try
      {
         return configuration.getTransformerClassName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getUniqueName()
   {
      clearIO();
      try
      {
         return divert.getUniqueName().toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isExclusive()
   {
      clearIO();
      try
      {
         return divert.isExclusive();
      }
      finally
      {
         blockOnIO();
      }
   }

   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(DivertControl.class);
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
