/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.prometheus;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jboss.logging.Logger;

class JmxScraper {

   private static final Logger logger = Logger.getLogger(JmxScraper.class.getName());

   public interface MBeanReceiver {

      void recordBean(String domain,
                      LinkedHashMap<String, String> beanProperties,
                      LinkedList<String> attrKeys,
                      String attrName,
                      String attrType,
                      String attrDescription,
                      Object value);
   }

   private final MBeanReceiver receiver;
   private MBeanServer mBeanServer;
   private final List<ObjectName> whitelistObjectNames, blacklistObjectNames;
   private final JmxMBeanPropertyCache jmxMBeanPropertyCache;

   JmxScraper(MBeanServer mBeanServer,
              MBeanReceiver receiver,
              JmxMBeanPropertyCache jmxMBeanPropertyCache,
              List<ObjectName> whitelistObjectNames,
              List<ObjectName> blacklistObjectNames) {
      this.receiver = receiver;
      this.whitelistObjectNames = whitelistObjectNames;
      this.blacklistObjectNames = blacklistObjectNames;
      this.jmxMBeanPropertyCache = jmxMBeanPropertyCache;
      this.mBeanServer = mBeanServer;
   }

   /**
    * Get a list of mbeans and scrape their values.
    *
    * Values are passed to the receiver in a single thread.
    */
   public void doScrape() throws Exception {
      // Query MBean names, see #89 for reasons queryMBeans() is used instead of queryNames()
      Set<ObjectName> mBeanNames = new HashSet<>();
      if (whitelistObjectNames == null || whitelistObjectNames.size() == 0) {
         for (ObjectInstance instance : mBeanServer.queryMBeans(null, null)) {
            mBeanNames.add(instance.getObjectName());
         }
      } else {
         for (ObjectName name : whitelistObjectNames) {
            for (ObjectInstance instance : mBeanServer.queryMBeans(name, null)) {
               mBeanNames.add(instance.getObjectName());
            }
         }
      }

      for (ObjectName name : blacklistObjectNames) {
         for (ObjectInstance instance : mBeanServer.queryMBeans(name, null)) {
            mBeanNames.remove(instance.getObjectName());
         }
      }

      // Now that we have *only* the whitelisted mBeans, remove any old ones from the cache:
      jmxMBeanPropertyCache.onlyKeepMBeans(mBeanNames);

      for (ObjectName objectName : mBeanNames) {
         long start = System.nanoTime();
         scrapeBean(mBeanServer, objectName);
         logger.trace("TIME: " + (System.nanoTime() - start) + " ns for " + objectName.toString());
      }
   }

   private void scrapeBean(MBeanServerConnection beanConn, ObjectName mbeanName) {
      MBeanInfo info;
      try {
         info = beanConn.getMBeanInfo(mbeanName);
      } catch (IOException e) {
         logScrape(mbeanName.toString(), "getMBeanInfo Fail: " + e);
         return;
      } catch (JMException e) {
         logScrape(mbeanName.toString(), "getMBeanInfo Fail: " + e);
         return;
      }
      MBeanAttributeInfo[] attrInfos = info.getAttributes();

      Map<String, MBeanAttributeInfo> name2AttrInfo = new LinkedHashMap<>();
      for (int idx = 0; idx < attrInfos.length; ++idx) {
         MBeanAttributeInfo attr = attrInfos[idx];
         if (!attr.isReadable()) {
            logScrape(mbeanName, attr, "not readable");
            continue;
         }
         name2AttrInfo.put(attr.getName(), attr);
      }
      final AttributeList attributes;
      try {
         attributes = beanConn.getAttributes(mbeanName, name2AttrInfo.keySet().toArray(new String[0]));
      } catch (Exception e) {
         logScrape(mbeanName, name2AttrInfo.keySet(), "Fail: " + e);
         return;
      }
      for (Attribute attribute : attributes.asList()) {
         MBeanAttributeInfo attr = name2AttrInfo.get(attribute.getName());
         logScrape(mbeanName, attr, "process");
         processBeanValue(mbeanName.getDomain(), jmxMBeanPropertyCache.getKeyPropertyList(mbeanName), new LinkedList<String>(), attr.getName(), attr.getType(), attr.getDescription(), attribute.getValue());
      }
   }

   /**
    * Recursive function for exporting the values of an mBean.
    * JMX is a very open technology, without any prescribed way of declaring mBeans
    * so this function tries to do a best-effort pass of getting the values/names
    * out in a way it can be processed elsewhere easily.
    */
   private void processBeanValue(String domain,
                                 LinkedHashMap<String, String> beanProperties,
                                 LinkedList<String> attrKeys,
                                 String attrName,
                                 String attrType,
                                 String attrDescription,
                                 Object value) {
      if (value == null) {
         logScrape(domain + beanProperties + attrName, "null");
      } else if (value instanceof Number || value instanceof String || value instanceof Boolean) {
         logScrape(domain + beanProperties + attrName, value.toString());
         this.receiver.recordBean(domain, beanProperties, attrKeys, attrName, attrType, attrDescription, value);
      } else if (value instanceof CompositeData) {
         logScrape(domain + beanProperties + attrName, "compositedata");
         CompositeData composite = (CompositeData) value;
         CompositeType type = composite.getCompositeType();
         attrKeys = new LinkedList<String>(attrKeys);
         attrKeys.add(attrName);
         for (String key : type.keySet()) {
            String typ = type.getType(key).getTypeName();
            Object valu = composite.get(key);
            processBeanValue(domain, beanProperties, attrKeys, key, typ, type.getDescription(), valu);
         }
      } else if (value instanceof TabularData) {
         // I don't pretend to have a good understanding of TabularData.
         // The real world usage doesn't appear to match how they were
         // meant to be used according to the docs. I've only seen them
         // used as 'key' 'value' pairs even when 'value' is itself a
         // CompositeData of multiple values.
         logScrape(domain + beanProperties + attrName, "tabulardata");
         TabularData tds = (TabularData) value;
         TabularType tt = tds.getTabularType();

         List<String> rowKeys = tt.getIndexNames();

         CompositeType type = tt.getRowType();
         Set<String> valueKeys = new TreeSet<>(type.keySet());
         valueKeys.removeAll(rowKeys);

         LinkedList<String> extendedAttrKeys = new LinkedList<>(attrKeys);
         extendedAttrKeys.add(attrName);
         for (Object valu : tds.values()) {
            if (valu instanceof CompositeData) {
               CompositeData composite = (CompositeData) valu;
               LinkedHashMap<String, String> l2s = new LinkedHashMap<>(beanProperties);
               for (String idx : rowKeys) {
                  Object obj = composite.get(idx);
                  if (obj != null) {
                     // Nested tabulardata will repeat the 'key' label, so
                     // append a suffix to distinguish each.
                     while (l2s.containsKey(idx)) {
                        idx = idx + "_";
                     }
                     l2s.put(idx, obj.toString());
                  }
               }
               for (String valueIdx : valueKeys) {
                  LinkedList<String> attrNames = extendedAttrKeys;
                  String typ = type.getType(valueIdx).getTypeName();
                  String name = valueIdx;
                  if (valueIdx.toLowerCase().equals("value")) {
                     // Skip appending 'value' to the name
                     attrNames = attrKeys;
                     name = attrName;
                  }
                  processBeanValue(domain, l2s, attrNames, name, typ, type.getDescription(), composite.get(valueIdx));
               }
            } else {
               logScrape(domain, "not a correct tabulardata format");
            }
         }
      } else if (value.getClass().isArray()) {
         logScrape(domain, "arrays are unsupported");
      } else {
         logScrape(domain + beanProperties, attrType + " is not exported");
      }
   }

   /**
    * For debugging.
    */
   private static void logScrape(ObjectName mbeanName, Set<String> names, String msg) {
      logScrape(mbeanName + "_" + names, msg);
   }

   private static void logScrape(ObjectName mbeanName, MBeanAttributeInfo attr, String msg) {
      logScrape(mbeanName + "'_'" + attr.getName(), msg);
   }

   private static void logScrape(String name, String msg) {
      logger.log(Logger.Level.TRACE, "scrape: '" + name + "': " + msg);
   }
}