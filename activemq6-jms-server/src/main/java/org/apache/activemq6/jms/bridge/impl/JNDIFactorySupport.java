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
package org.apache.activemq6.jms.bridge.impl;

import java.util.Hashtable;

import javax.naming.InitialContext;

/**
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>10 Oct 2007
 *
 *
 */
public abstract class JNDIFactorySupport
{
   protected Hashtable jndiProperties;

   protected String lookup;

   protected JNDIFactorySupport(final Hashtable jndiProperties, final String lookup)
   {
      this.jndiProperties = jndiProperties;

      this.lookup = lookup;
   }

   protected Object createObject() throws Exception
   {
      InitialContext ic = null;

      Object obj = null;

      try
      {
         if (jndiProperties == null)
         {
            ic = new InitialContext();
         }
         else
         {
            ic = new InitialContext(jndiProperties);
         }

         obj = ic.lookup(lookup);
      }
      finally
      {
         if (ic != null)
         {
            ic.close();
         }
      }
      return obj;
   }
}
