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
package org.hornetq.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ObjectInputStreamWithClassLoader extends ObjectInputStream
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ObjectInputStreamWithClassLoader(final InputStream in) throws IOException
   {
      super(in);
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   @Override
   protected Class resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException
   {
      if (System.getSecurityManager() == null)
      {
         return resolveClass0(desc);
      }
      else
      {
         try
         {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Class>()
            {
               @Override
               public Class run() throws Exception
               {
                  return resolveClass0(desc);
               }
            });
         }
         catch (PrivilegedActionException e)
         {
            throw unwrapException(e);
         }
      }
   }

   @Override
   protected Class resolveProxyClass(final String[] interfaces) throws IOException, ClassNotFoundException
   {
      if (System.getSecurityManager() == null)
      {
         return resolveProxyClass0(interfaces);
      }
      else
      {
         try
         {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Class>()
            {
               @Override
               public Class run() throws Exception
               {
                  return resolveProxyClass0(interfaces);
               }
            });
         }
         catch (PrivilegedActionException e)
         {
            throw unwrapException(e);
         }
      }
   }

   // Private --------------------------------------------------------------------------------------

   private Class resolveClass0(final ObjectStreamClass desc) throws IOException, ClassNotFoundException
   {
      String name = desc.getName();
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         // HORNETQ-747 https://issues.jboss.org/browse/HORNETQ-747
         // Use Class.forName instead of ClassLoader.loadClass to avoid issues with loading arrays
         Class clazz = Class.forName(name, false, loader);
         // sanity check only.. if a classLoader can't find a clazz, it will throw an exception
         if (clazz == null)
         {
            return super.resolveClass(desc);
         }
         else
         {
            return clazz;
         }
      }
      catch (ClassNotFoundException e)
      {
         return super.resolveClass(desc);
      }
   }

   private Class resolveProxyClass0(String[] interfaces) throws IOException, ClassNotFoundException
   {
      ClassLoader latestLoader = Thread.currentThread().getContextClassLoader();
      ClassLoader nonPublicLoader = null;
      boolean hasNonPublicInterface = false;
      // define proxy in class loader of non-public interface(s), if any
      Class[] classObjs = new Class[interfaces.length];
      for (int i = 0; i < interfaces.length; i++)
      {
         Class cl = Class.forName(interfaces[i], false, latestLoader);
         if ((cl.getModifiers() & Modifier.PUBLIC) == 0)
         {
            if (hasNonPublicInterface)
            {
               if (nonPublicLoader != cl.getClassLoader())
               {
                  throw new IllegalAccessError("conflicting non-public interface class loaders");
               }
            }
            else
            {
               nonPublicLoader = cl.getClassLoader();
               hasNonPublicInterface = true;
            }
         }
         classObjs[i] = cl;
      }
      try
      {
         return Proxy.getProxyClass(hasNonPublicInterface ? nonPublicLoader : latestLoader, classObjs);
      }
      catch (IllegalArgumentException e)
      {
         throw new ClassNotFoundException(null, e);
      }
   }

   private RuntimeException unwrapException(PrivilegedActionException e) throws IOException, ClassNotFoundException
   {
      Throwable c = e.getCause();
      if (c instanceof IOException)
      {
         throw (IOException)c;
      }
      else if (c instanceof ClassNotFoundException)
      {
         throw (ClassNotFoundException)c;
      }
      else if (c instanceof RuntimeException)
      {
         throw (RuntimeException)c;
      }
      else if (c instanceof Error)
      {
         throw (Error)c;
      }
      else
      {
         throw new RuntimeException(c);
      }
   }

   // Inner classes --------------------------------------------------------------------------------

}
