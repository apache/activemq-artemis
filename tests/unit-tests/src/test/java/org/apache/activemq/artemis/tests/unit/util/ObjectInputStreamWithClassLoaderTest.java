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
package org.apache.activemq.artemis.tests.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.jms.client.ObjectInputFilterFactory;
import org.apache.activemq.artemis.ra.ConnectionFactoryProperties;
import org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass;
import org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;
import org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.apache.activemq.artemis.utils.ObjectInputStreamWithClassLoader;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ObjectInputStreamWithClassLoaderTest extends ActiveMQTestBase {

   public static ClassLoader newClassLoader(final Class... userClasses) throws Exception {

      Set<URL> userClassUrls = new HashSet<>();
      for (Class anyUserClass : userClasses) {
         ProtectionDomain protectionDomain = anyUserClass.getProtectionDomain();
         CodeSource codeSource = protectionDomain.getCodeSource();
         URL classLocation = codeSource.getLocation();
         userClassUrls.add(classLocation);
      }
      StringTokenizer tokenString = new StringTokenizer(System.getProperty("java.class.path"), File.pathSeparator);
      String pathIgnore = System.getProperty("java.home");
      if (pathIgnore == null) {
         pathIgnore = userClassUrls.iterator().next().toString();
      }

      List<URL> urls = new ArrayList<>();
      while (tokenString.hasMoreElements()) {
         String value = tokenString.nextToken();
         URL itemLocation = new File(value).toURI().toURL();
         if (!userClassUrls.contains(itemLocation) && itemLocation.toString().indexOf(pathIgnore) >= 0) {
            urls.add(itemLocation);
         }
      }
      URL[] urlArray = urls.toArray(new URL[urls.size()]);

      ClassLoader mainClassLoader = URLClassLoader.newInstance(urlArray, null);
      ClassLoader appClassLoader = URLClassLoader.newInstance(userClassUrls.toArray(new URL[0]), mainClassLoader);
      return appClassLoader;
   }


   @Test
   public void testClassLoaderIsolation() throws Exception {

      ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try {
         AnObject obj = new AnObjectImpl();
         byte[] bytes = ObjectInputStreamWithClassLoaderTest.toBytes(obj);

         //Class.isAnonymousClass() call used in ObjectInputStreamWithClassLoader
         //need to access the enclosing class and its parent class of the obj
         //i.e. ActiveMQTestBase, ArtemisTestCase, and Assert.
         ClassLoader testClassLoader = ObjectInputStreamWithClassLoaderTest.newClassLoader(obj.getClass(), ActiveMQTestBase.class, ArtemisTestCase.class);
         Thread.currentThread().setContextClassLoader(testClassLoader);

         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);

         Object deserializedObj = ois.readObject();

         assertNotSame(obj, deserializedObj);
         assertNotSame(obj.getClass(), deserializedObj.getClass());
         assertNotSame(obj.getClass().getClassLoader(), deserializedObj.getClass().getClassLoader());
         assertSame(testClassLoader, deserializedObj.getClass().getClassLoader());
      } finally {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   @Test
   public void testClassLoaderIsolationWithProxy() throws Exception {

      ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try {
         AnObject originalProxy = (AnObject) Proxy.newProxyInstance(AnObject.class.getClassLoader(), new Class[]{AnObject.class}, new AnObjectInvocationHandler());
         originalProxy.setMyInt(100);
         byte[] bytes = ObjectInputStreamWithClassLoaderTest.toBytes(originalProxy);

         ClassLoader testClassLoader = ObjectInputStreamWithClassLoaderTest.newClassLoader(this.getClass(), ActiveMQTestBase.class, ArtemisTestCase.class);
         Thread.currentThread().setContextClassLoader(testClassLoader);
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);

         Runnable toRun = (Runnable) testClassLoader.loadClass(ProxyReader.class.getName()).getDeclaredConstructor().newInstance();
         toRun.getClass().getField("ois").set(toRun, ois);
         toRun.getClass().getField("testClassLoader").set(toRun, testClassLoader);
         toRun.getClass().getField("originalProxy").set(toRun, originalProxy);

         toRun.run();

      } finally {
         Thread.currentThread().setContextClassLoader(originalClassLoader);
      }

   }

   @Test
   public void testAllowDenyList() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      //default
      assertNull(readSerializedObject(null, null, serailizeFile));

      //allow list
      String allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization";
      assertNull(readSerializedObject(allowList, null, serailizeFile));
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      assertNull(readSerializedObject(allowList, null, serailizeFile));

      allowList = "some.other.package";
      Exception result = readSerializedObject(allowList, null, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //denyList
      String denyList = "org.apache.activemq.artemis.tests.unit.util";
      result = readSerializedObject(null, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(null, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg2";
      result = readSerializedObject(null, denyList,serailizeFile);
      assertNull(result);

      denyList = "some.other.package";
      allowList = "some.other.package1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //denyList priority
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1, some.other.package";
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      denyList = "org.apache.activemq.artemis.tests.unit, some.other.package";
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.pkg2, some.other.package";
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

      denyList = "some.other.package, org.apache.activemq.artemis.tests.unit.util.deserialization.pkg2";
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

      //wildcard
      denyList = "*";
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      denyList = "*";
      allowList = "*";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);
      result = readSerializedObject(allowList, null, serailizeFile);
      assertNull(result);
   }

   @Test
   public void testAllowDenyListAgainstArrayObject() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      TestClass1[] sourceObject = new TestClass1[]{new TestClass1()};

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      //default ok
      String denyList = null;
      String allowList = null;

      Object result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

      //now denylist TestClass1
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      allowList = null;

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now allowlist TestClass1, it should pass.
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);
   }

   @Test
   public void testAllowDenyListAgainstListObject() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      List<TestClass1> sourceObject = new ArrayList<>();
      sourceObject.add(new TestClass1());

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      //default ok
      String denyList = null;
      String allowList = null;

      Object result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

      //now denylist TestClass1
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      allowList = null;

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now allowlist TestClass1, should fail because the List type is not allowed
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now add List to allow list, it should pass
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1," + "java.util.ArrayList";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

   }

   @Test
   public void testAllowDenyListAgainstListMapObject() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      Map<TestClass1, TestClass2> sourceObject = new HashMap<>();
      sourceObject.put(new TestClass1(), new TestClass2());

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String denyList = null;
      String allowList = null;

      Object result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

      //now denylist the key
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      allowList = null;

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now denylist the value
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2";
      allowList = null;

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now allowlist the key, should fail too because value is forbidden
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now allowlist the value, should fail too because the key is forbidden
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //both key and value are in the allowlist, it should fail because HashMap not permitted
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1," + "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //now add HashMap, test should pass.
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1," +
         "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2," +
         "java.util.HashMap";

      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);

   }

   @Test
   public void testAllowDenyListAnonymousObject() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         Serializable object = EnclosingClass.anonymousObject;
         assertTrue(object.getClass().isAnonymousClass());
         outputStream.writeObject(object);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      //default
      String denyList = null;
      String allowList = null;
      assertNull(readSerializedObject(allowList, denyList, serailizeFile));

      //forbidden by specifying the enclosing class
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass";
      Object result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //do it in allowlist
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);
   }

   @Test
   public void testAllowDenyListLocalObject() throws Exception {
      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         Object object = EnclosingClass.getLocalObject();
         assertTrue(object.getClass().isLocalClass());
         outputStream.writeObject(object);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      //default
      String denyList = null;
      String allowList = null;
      assertNull(readSerializedObject(allowList, denyList, serailizeFile));

      //forbidden by specifying the enclosing class
      denyList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass";
      Object result = readSerializedObject(allowList, denyList, serailizeFile);
      assertTrue(result instanceof ClassNotFoundException);

      //do it in allowList
      denyList = null;
      allowList = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass";
      result = readSerializedObject(allowList, denyList, serailizeFile);
      assertNull(result);
   }

   @Test
   public void testDeprecatedWhiteBlackListSystemProperty() throws Exception {

      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      System.setProperty(ObjectInputStreamWithClassLoader.BLACKLIST_PROPERTY, "system.defined.black.list");
      System.setProperty(ObjectInputStreamWithClassLoader.WHITELIST_PROPERTY, "system.defined.white.list");
      try {
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(new FileInputStream(serailizeFile));
         String bList = ois.getDenyList();
         String wList = ois.getAllowList();
         assertEquals("system.defined.black.list", bList, "wrong black list: " + bList);
         assertEquals("system.defined.white.list", wList, "wrong white list: " + wList);
         ois.close();
      } finally {
         System.clearProperty(ObjectInputStreamWithClassLoader.BLACKLIST_PROPERTY);
         System.clearProperty(ObjectInputStreamWithClassLoader.WHITELIST_PROPERTY);
      }
   }

   @Test
   public void testAllowDenyListSystemProperty() throws Exception {

      File serailizeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serailizeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      System.setProperty(ObjectInputStreamWithClassLoader.DENYLIST_PROPERTY, "system.defined.deny.list");
      System.setProperty(ObjectInputStreamWithClassLoader.ALLOWLIST_PROPERTY, "system.defined.allow.list");
      try {
         ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(new FileInputStream(serailizeFile));
         String bList = ois.getDenyList();
         String wList = ois.getAllowList();
         assertEquals("system.defined.deny.list", bList, "wrong deny list: " + bList);
         assertEquals("system.defined.allow.list", wList, "wrong allow list: " + wList);
         ois.close();
      } finally {
         System.clearProperty(ObjectInputStreamWithClassLoader.DENYLIST_PROPERTY);
         System.clearProperty(ObjectInputStreamWithClassLoader.ALLOWLIST_PROPERTY);
      }
   }

   // serialFilter version of testAllowDenyList()
   @Test
   public void testSerialFilter() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // Allow all
      assertNull(readSerializedObjectSF("*", serializeFile));

      // Reject all
      result = readSerializedObjectSF("!*", serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // allow list

      // sub-package
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.**;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);

      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // package
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);

      // other package
      serialFilter = "some.other.package.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // deny list

      // sub-package
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.**";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // package
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // other package
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg2.*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);


      serialFilter = "!some.other.package.*;some.other.package1.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);


      // deny list priority
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*;!some.other.package.*;" +
                     "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*;*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.pkg2.*;!some.other.package.*;" +
                     "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);

      serialFilter = "!some.other.package.*;!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg2.*;" +
                     "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);


      // wildcard
      serialFilter = "!*;org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      serialFilter = "!*;*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      serialFilter = "*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   // serialFilter version of testAllowDenyListAgainstArrayObject()
   @Test
   public void testSerialFilterAgainstArrayObject() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      TestClass1[] sourceObject = new TestClass1[]{new TestClass1()};

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // now deny TestClass1
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now allow TestClass1, it should pass.

      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   // serialFilter version of testAllowDenyListAgainstListObject()
   @Test
   public void testSerialFilterListAgainstListObject() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      List<TestClass1> sourceObject = new ArrayList<>();
      sourceObject.add(new TestClass1());

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // now deny TestClass1
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now allow TestClass1, should fail because the List type is not allowed
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now add List to allow list, it should pass
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;java.util.ArrayList;java.lang.Object;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   // serialFilter version of testAllowDenyListAgainstListMapObject()
   @Test
   public void testSerialFilterAgainstListMapObject() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      Map<TestClass1, TestClass2> sourceObject = new HashMap<>();
      sourceObject.put(new TestClass1(), new TestClass2());

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(sourceObject);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // now deny TestClass1 - key
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now deny TestClass2 - value
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now allow the key, should fail too because value is forbidden (and HashMap)
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now allow the value, should fail too because the key is forbidden (and HashMap)
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // both key and value are in the allow list, it should fail because HashMap not permitted
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;" +
                     "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // now add HashMap, test should pass.
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;" +
                     "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass2;" +
                     "java.util.HashMap;java.util.Map$Entry;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   // serialFilter version of testAllowDenyListAnonymousObject()
   @Test
   public void testSerialFilterAnonymousObject() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         Serializable object = EnclosingClass.anonymousObject;
         assertTrue(object.getClass().isAnonymousClass());
         outputStream.writeObject(object);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // forbidden by specifying the enclosing class
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // do it in allow List
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   // serialFilter version of testAllowDenyListLocalObject()
   @Test
   public void testSerialFilterLocalObject() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");

      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         Object object = EnclosingClass.getLocalObject();
         assertTrue(object.getClass().isLocalClass());
         outputStream.writeObject(object);
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      String    serialFilter;
      Exception result;

      // default
      assertNull(readSerializedObjectSF(null, serializeFile));

      // forbidden by specifying the enclosing class
      serialFilter = "!org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertTrue(result instanceof InvalidClassException);

      // do it in allow List
      serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.EnclosingClass*;!*";
      result = readSerializedObjectSF(serialFilter, serializeFile);
      assertNull(result);
   }

   @Test
   public void testSerialFilterSystemProperty() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      ConnectionFactoryOptions options = new ConnectionFactoryProperties();

      Exception result;

      // default
      assertNull(readSerializedObject(options, serializeFile));

      // Set system property filter to block everything
      System.setProperty(ObjectInputFilterFactory.SERIAL_FILTER_PROPERTY, "!*");
      try {
         // Default filter should be read from system property
         result = readSerializedObject(options, serializeFile);
         assertTrue(result instanceof InvalidClassException);

         // Set options to allow all, this should override the default system property
         options.setSerialFilter("*");
         assertNull(readSerializedObject(options, serializeFile));
      } finally {
         System.clearProperty(ObjectInputFilterFactory.SERIAL_FILTER_PROPERTY);
      }
   }

   @Test
   public void testObjectInputFilterFactoryPatternCache() {
      String serialFilter = "org.apache.activemq.artemis.tests.unit.util.deserialization.pkg1.TestClass1;!*";

      ObjectInputFilter oif1 = ObjectInputFilterFactory.getObjectInputFilterForPattern(serialFilter);
      ObjectInputFilter oif2 = ObjectInputFilterFactory.getObjectInputFilterForPattern(serialFilter);
      ObjectInputFilter oif3 = ObjectInputFilterFactory.getObjectInputFilterForPattern(new String(serialFilter));

      assertSame(oif1, oif2);
      assertSame(oif1, oif3);
   }

   @Test
   public void testObjectInputFilterFactoryClassNameCache() {
      String className = AlwaysRejectObjectInputFilter.class.getName();

      ObjectInputFilter oif1 = ObjectInputFilterFactory.getObjectInputFilterForClassName(className);
      ObjectInputFilter oif2 = ObjectInputFilterFactory.getObjectInputFilterForClassName(className);
      ObjectInputFilter oif3 = ObjectInputFilterFactory.getObjectInputFilterForClassName(new String(className));

      assertSame(oif1, oif2);
      assertSame(oif1, oif3);
   }

   @Test
   public void testObjectInputFilter() throws Exception {
      File serializeFile = new File(temporaryFolder, "testclass.bin");
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(serializeFile));
      try {
         outputStream.writeObject(new TestClass1());
         outputStream.flush();
      } finally {
         outputStream.close();
      }

      ConnectionFactoryOptions options = new ConnectionFactoryProperties();

      Exception result;

      // default
      assertNull(readSerializedObject(options, serializeFile));

      // always accept filter
      options.setSerialFilterClassName(AlwaysAcceptObjectInputFilter.class.getName());
      assertNull(readSerializedObject(options, serializeFile));

      // always reject filter
      options.setSerialFilterClassName(AlwaysRejectObjectInputFilter.class.getName());
      result = readSerializedObject(options, serializeFile);
      assertTrue(result instanceof InvalidClassException);
   }

   private Exception readSerializedObject(String allowList, String denyList, File serailizeFile) {
      Exception result = null;

      ObjectInputStreamWithClassLoader ois = null;

      try {
         ois = new ObjectInputStreamWithClassLoader(new FileInputStream(serailizeFile));
         ois.setAllowList(allowList);
         ois.setDenyList(denyList);
         ois.readObject();
      } catch (Exception e) {
         result = e;
      } finally {
         try {
            ois.close();
         } catch (IOException e) {
            result = e;
         }
      }
      return result;
   }

   private Exception readSerializedObjectSF(String serialFilter, File serializeFile) {
      Exception result = null;

      ObjectInputStreamWithClassLoader ois = null;

      try {
         ois = new ObjectInputStreamWithClassLoader(new FileInputStream(serializeFile));

         ObjectInputFilter oif = ObjectInputFilterFactory.getObjectInputFilterForPattern(serialFilter);
         if (oif != null) {
            ois.setObjectInputFilter(new DebugObjectInputFilter(oif));
         }

         ois.readObject();
      } catch (Exception e) {
         result = e;
      } finally {
         try {
            ois.close();
         } catch (IOException e) {
            result = e;
         }
      }
      return result;
   }

   private Exception readSerializedObject(ConnectionFactoryOptions options, File serializeFile) {
      Exception result = null;

      ObjectInputStreamWithClassLoader ois = null;

      try {
         ois = new ObjectInputStreamWithClassLoader(new FileInputStream(serializeFile));

         ObjectInputFilter oif = ObjectInputFilterFactory.getObjectInputFilter(options);
         if (oif != null) {
            ois.setObjectInputFilter(new DebugObjectInputFilter(oif));
         }

         ois.readObject();
      } catch (Exception e) {
         result = e;
      } finally {
         try {
            ois.close();
         } catch (IOException e) {
            result = e;
         }
      }
      return result;
   }

   public static class ProxyReader implements Runnable {

      public java.io.ObjectInputStream ois;
      public Object originalProxy;
      public ClassLoader testClassLoader;

      // We don't have access to the junit framework on the classloader where this is running
      void myAssertNotSame(Object obj, Object obj2) {
         if (obj == obj2) {
            throw new RuntimeException("Expected to be different objects");
         }
      }

      // We don't have access to the junit framework on the classloader where this is running
      void myAssertSame(Object obj, Object obj2) {
         if (obj != obj2) {
            throw new RuntimeException("Expected to be the same objects");
         }
      }

      @Override
      public void run() {

         try {
            Object deserializedObj = ois.readObject();

            // cannot use logging here, as this test is playing with classLoading here.
            System.out.println("Deserialized Object " + deserializedObj);

            myAssertNotSame(originalProxy, deserializedObj);
            myAssertNotSame(originalProxy.getClass(), deserializedObj.getClass());
            myAssertNotSame(originalProxy.getClass().getClassLoader(), deserializedObj.getClass().getClassLoader());
            myAssertSame(testClassLoader, deserializedObj.getClass().getClassLoader());

            AnObject myInterface = (AnObject) deserializedObj;

            if (myInterface.getMyInt() != 200) {
               throw new RuntimeException("invalid result");
            }
         } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
         } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
         }

      }
   }

   private static byte[] toBytes(final Object obj) throws IOException {
      assertTrue(obj instanceof Serializable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();
   }


   private interface AnObject extends Serializable {

      int getMyInt();

      void setMyInt(int value);

      long getMyLong();

      void setMyLong(long value);
   }

   private static class AnObjectImpl implements AnObject {

      private static final long serialVersionUID = -5172742084489525256L;

      int myInt = 0;
      long myLong = 0L;

      @Override
      public int getMyInt() {
         return myInt;
      }

      @Override
      public void setMyInt(int value) {
         this.myInt = value;
      }

      @Override
      public long getMyLong() {
         return myLong;
      }

      @Override
      public void setMyLong(long value) {
         this.myLong = value;
      }
   }

   private static class AnObjectInvocationHandler implements InvocationHandler, Serializable {

      private static final long serialVersionUID = -3875973764178767452L;
      private final AnObject anObject = new AnObjectImpl();

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
         Object obj = method.invoke(anObject, args);
         if (obj instanceof Integer) {
            return ((Integer) obj).intValue() * 2;
         } else {
            return obj;
         }

      }
   }

   private static class DebugObjectInputFilter implements ObjectInputFilter {
      private static final Logger logger = LoggerFactory.getLogger("objectfilter");

      private ObjectInputFilter delegate;

      DebugObjectInputFilter(ObjectInputFilter delegate) {
         this.delegate = delegate;
      }

      @Override
      public Status checkInput(FilterInfo filterInfo) {
         Status status = delegate != null ? delegate.checkInput(filterInfo) : Status.UNDECIDED;

         if (logger.isDebugEnabled()) {
            logger.debug("checkInput(): serialClass = {}, arrayLength = {}, depth = {}, references = {}, streamBytes = {}, STATUS = {}",
                         filterInfo.serialClass(),
                         filterInfo.arrayLength(),
                         filterInfo.depth(),
                         filterInfo.references(),
                         filterInfo.streamBytes(),
                         status);
         }

         return status;
      }
   }

   public static class AlwaysRejectObjectInputFilter implements ObjectInputFilter {
      @Override
      public Status checkInput(FilterInfo filterInfo) {
         return Status.REJECTED;
      }
   }

   public static class AlwaysAcceptObjectInputFilter implements ObjectInputFilter {
      @Override
      public Status checkInput(FilterInfo filterInfo) {
         return Status.ALLOWED;
      }
   }
}
