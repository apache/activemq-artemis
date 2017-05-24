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
package org.apache.activemq.artemis.component;

import org.eclipse.jetty.webapp.WebAppClassLoader;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.jar.JarFile;

/**
 * This classloader manually closes any JarFiles in its parent
 * class loader to prevent inputstream leaks that prevents
 * jar files in webapp's temp dir from being cleaned up.
 */
public class CustomizedWebAppClassloader extends WebAppClassLoader {

   private HashSet<String> leakedJarNames = new HashSet<>();

   public CustomizedWebAppClassloader(Context context) throws IOException {
      super(context);
   }

   @Override
   public void close() {
      leakedJarNames.clear();
      preClose(this);
      cleanupJarFileFactory();

      try {
         super.close();
      } catch (IOException e) {
      }
   }

   private Field getClassField(Class clz, String fieldName) {
      Field field = null;
      try {
         field = clz.getDeclaredField(fieldName);
         field.setAccessible(true);
      } catch (NoSuchFieldException e) {
      }
      return field;
   }

   private void preClose(ClassLoader cl) {
      Field f = getClassField(URLClassLoader.class, "ucp");
      if (f != null) {
         Object obj = null;
         try {
            obj = f.get(cl);
            final Object ucp = obj;
            f = getClassField(ucp.getClass(), "loaders");
            if (f != null) {
               ArrayList loaders = null;
               try {
                  loaders = (ArrayList) f.get(ucp);
               } catch (IllegalAccessException ex) {
               }
               for (int i = 0; loaders != null && i < loaders.size(); i++) {
                  obj = loaders.get(i);
                  f = getClassField(obj.getClass(), "jar");
                  if (f != null) {
                     try {
                        obj = f.get(obj);
                     } catch (IllegalAccessException ex) {
                     }
                     if (obj instanceof JarFile) {
                        final JarFile jarFile = (JarFile) obj;
                        leakedJarNames.add(jarFile.getName());
                        try {
                           jarFile.close();
                        } catch (IOException ex) {
                        }
                     }
                  }
               }
            }
         } catch (IllegalAccessException ex) {
         }
      }
   }

   private void cleanupJarFileFactory() {
      Class classJarURLConnection = null;
      try {
         classJarURLConnection = Class.forName("sun.net.www.protocol.jar.JarURLConnection");
      } catch (ClassNotFoundException ex) {
         return;
      }

      Field f = getClassField(classJarURLConnection, "factory");

      if (f == null) {
         return;
      }
      Object obj = null;
      try {
         obj = f.get(null);
      } catch (IllegalAccessException ex) {
         return;
      }

      Class classJarFileFactory = obj.getClass();
      HashMap fileCache = null;
      f = getClassField(classJarFileFactory, "fileCache");
      if (f == null) {
         return;
      }
      try {
         obj = f.get(null);
         if (obj instanceof HashMap) {
            fileCache = (HashMap) obj;
         }
      } catch (IllegalAccessException ex) {
      }
      HashMap urlCache = null;
      f = getClassField(classJarFileFactory, "urlCache");
      if (f == null) {
         return;
      }
      try {
         obj = f.get(null);
         if (obj instanceof HashMap) {
            urlCache = (HashMap) obj;
         }
      } catch (IllegalAccessException ex) {
      }
      if (urlCache != null) {
         HashMap urlCacheTmp = (HashMap) urlCache.clone();
         Iterator it = urlCacheTmp.keySet().iterator();
         while (it.hasNext()) {
            obj = it.next();
            if (!(obj instanceof JarFile)) {
               continue;
            }
            JarFile jarFile = (JarFile) obj;
            if (leakedJarNames.contains(jarFile.getName())) {
               try {
                  jarFile.close();
               } catch (IOException ex) {
               }
               if (fileCache != null) {
                  fileCache.remove(urlCache.get(jarFile));
               }
               urlCache.remove(jarFile);
            }
         }
      } else if (fileCache != null) {
         HashMap fileCacheTmp = (HashMap) fileCache.clone();
         Iterator it = fileCacheTmp.keySet().iterator();
         while (it.hasNext()) {
            Object key = it.next();
            obj = fileCache.get(key);
            if (!(obj instanceof JarFile)) {
               continue;
            }
            JarFile jarFile = (JarFile) obj;
            if (leakedJarNames.contains(jarFile.getName())) {
               try {
                  jarFile.close();
               } catch (IOException ex) {
               }
               fileCache.remove(key);
            }
         }
      }
      leakedJarNames.clear();
   }

}