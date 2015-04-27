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
package org.apache.activemq.artemis.tests.unit.util;

import org.apache.activemq.artemis.tests.util.UnitTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.junit.Assert;

import org.apache.activemq.artemis.utils.ObjectInputStreamWithClassLoader;

public class ObjectInputStreamWithClassLoaderTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ClassLoader newClassLoader(final Class anyUserClass) throws Exception
   {
      ProtectionDomain protectionDomain = anyUserClass.getProtectionDomain();
      CodeSource codeSource = protectionDomain.getCodeSource();
      URL classLocation = codeSource.getLocation();
      StringTokenizer tokenString = new StringTokenizer(System.getProperty("java.class.path"), File.pathSeparator);
      String pathIgnore = System.getProperty("java.home");
      if (pathIgnore == null)
      {
         pathIgnore = classLocation.toString();
      }

      List<URL> urls = new ArrayList<URL>();
      while (tokenString.hasMoreElements())
      {
         String value = tokenString.nextToken();
         URL itemLocation = new File(value).toURI().toURL();
         if (!itemLocation.equals(classLocation) && itemLocation.toString().indexOf(pathIgnore) >= 0)
         {
            urls.add(itemLocation);
         }
      }

      URL[] urlArray = urls.toArray(new URL[urls.size()]);

      ClassLoader masterClassLoader = URLClassLoader.newInstance(urlArray, null);
      ClassLoader appClassLoader = URLClassLoader.newInstance(new URL[]{classLocation}, masterClassLoader);
      return appClassLoader;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testClassLoaderIsolation() throws Exception
   {

      ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try
      {
         AnObject obj = new AnObjectImpl();
         byte[] bytes = ObjectInputStreamWithClassLoaderTest.toBytes(obj);

         ClassLoader testClassLoader = ObjectInputStreamWithClassLoaderTest.newClassLoader(obj.getClass());
         Thread.currentThread().setContextClassLoader(testClassLoader);

         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);

         Object deserializedObj = ois.readObject();

         Assert.assertNotSame(obj, deserializedObj);
         Assert.assertNotSame(obj.getClass(), deserializedObj.getClass());
         Assert.assertNotSame(obj.getClass().getClassLoader(), deserializedObj.getClass().getClassLoader());
         Assert.assertSame(testClassLoader, deserializedObj.getClass().getClassLoader());
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   @Test
   public void testClassLoaderIsolationWithProxy() throws Exception
   {

      ClassLoader originalClassLoader = Thread.currentThread()
            .getContextClassLoader();
      try
      {
         AnObject originalProxy = (AnObject) Proxy.newProxyInstance(
               AnObject.class.getClassLoader(),
               new Class[]{AnObject.class},
               new AnObjectInvocationHandler());
         originalProxy.setMyInt(100);
         byte[] bytes = ObjectInputStreamWithClassLoaderTest
               .toBytes(originalProxy);

         ClassLoader testClassLoader = ObjectInputStreamWithClassLoaderTest
               .newClassLoader(this.getClass());
         Thread.currentThread().setContextClassLoader(testClassLoader);
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(
               bais);

         Runnable toRun = (Runnable) testClassLoader.loadClass(
               ProxyReader.class.getName()).newInstance();
         toRun.getClass().getField("ois").set(toRun, ois);
         toRun.getClass().getField("testClassLoader")
               .set(toRun, testClassLoader);
         toRun.getClass().getField("originalProxy")
               .set(toRun, originalProxy);

         toRun.run();

      }
      finally
      {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public static class ProxyReader implements Runnable
   {
      public java.io.ObjectInputStream ois;
      public Object originalProxy;
      public ClassLoader testClassLoader;

      // We don't have access to the junit framework on the classloader where this is running
      void myAssertNotSame(Object obj, Object obj2)
      {
         if (obj == obj2)
         {
            throw new RuntimeException("Expected to be different objects");
         }
      }

      // We don't have access to the junit framework on the classloader where this is running
      void myAssertSame(Object obj, Object obj2)
      {
         if (obj != obj2)
         {
            throw new RuntimeException("Expected to be the same objects");
         }
      }

      public void run()
      {

         try
         {
            Object deserializedObj = ois.readObject();

            System.out.println("Deserialized Object " + deserializedObj);

            myAssertNotSame(originalProxy, deserializedObj);
            myAssertNotSame(originalProxy.getClass(),
                  deserializedObj.getClass());
            myAssertNotSame(originalProxy.getClass().getClassLoader(),
                  deserializedObj.getClass().getClassLoader());
            myAssertSame(testClassLoader, deserializedObj.getClass()
                  .getClassLoader());

            AnObject myInterface = (AnObject) deserializedObj;

            if (myInterface.getMyInt() != 200)
            {
               throw new RuntimeException("invalid result");
            }
         }
         catch (ClassNotFoundException e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
         catch (IOException e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }

      }
   }

   private static byte[] toBytes(final Object obj) throws IOException
   {
      Assert.assertTrue(obj instanceof Serializable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();
   }

   // Inner classes -------------------------------------------------

   private interface AnObject extends Serializable
   {
      int getMyInt();

      void setMyInt(int value);

      long getMyLong();

      void setMyLong(long value);
   }

   private static class AnObjectImpl implements AnObject
   {
      private static final long serialVersionUID = -5172742084489525256L;

      int myInt = 0;
      long myLong = 0L;

      @Override
      public int getMyInt()
      {
         return myInt;
      }

      @Override
      public void setMyInt(int value)
      {
         this.myInt = value;
      }

      @Override
      public long getMyLong()
      {
         return myLong;
      }

      @Override
      public void setMyLong(long value)
      {
         this.myLong = value;
      }
   }

   private static class AnObjectInvocationHandler implements InvocationHandler, Serializable
   {
      private static final long serialVersionUID = -3875973764178767452L;
      private final AnObject anObject = new AnObjectImpl();

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
      {
         Object obj = method.invoke(anObject, args);
         if (obj instanceof Integer)
         {
            return ((Integer) obj).intValue() * 2;
         }
         else
         {
            return obj;
         }

      }
   }
}