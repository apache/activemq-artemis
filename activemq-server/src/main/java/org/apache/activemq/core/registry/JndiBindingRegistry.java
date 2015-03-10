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
package org.apache.activemq.core.registry;

import org.apache.activemq.spi.core.naming.BindingRegistry;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JndiBindingRegistry implements BindingRegistry
{
   private Context context;

   public JndiBindingRegistry(Context context)
   {
      this.context = context;
   }

   public JndiBindingRegistry() throws Exception
   {
      this.context = new InitialContext();
   }

   public Object lookup(String name)
   {
      try
      {
         if (context == null)
         {
            return null;
         }
         else
         {
            return context.lookup(name);
         }
      }
      catch (NamingException e)
      {
         return null;
      }
   }

   public boolean bind(String name, Object obj)
   {
      try
      {
         return bindToJndi(name, obj);
      }
      catch (NamingException e)
      {
         throw new RuntimeException(e);
      }
   }

   public void unbind(String name)
   {
      try
      {
         if (context != null)
         {
            context.unbind(name);
         }
      }
      catch (NamingException e)
      {
      }
   }

   public void close()
   {
      try
      {
         if (context != null)
         {
            context.close();
         }
      }
      catch (NamingException e)
      {
      }
   }


   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      if (context != null)
      {
         String parentContext;
         String jndiNameInContext;
         int sepIndex = jndiName.lastIndexOf('/');
         if (sepIndex == -1)
         {
            parentContext = "";
         }
         else
         {
            parentContext = jndiName.substring(0, sepIndex);
         }
         jndiNameInContext = jndiName.substring(sepIndex + 1);
         try
         {
            context.lookup(jndiName);

            //JMSServerManagerImpl.log.warn("Binding for " + jndiName + " already exists");
            return false;
         }
         catch (Throwable e)
         {
            // OK
         }

         Context c = org.apache.activemq.utils.JNDIUtil.createContext(context, parentContext);

         c.rebind(jndiNameInContext, objectToBind);
      }
      return true;
   }
}
