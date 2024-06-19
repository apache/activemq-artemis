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
package org.apache.activemq.artemis.core.remoting.impl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfigurationHelper;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.utils.ClassloadingUtil;

/**
 * Stores static mappings of class names to ConnectorFactory instances to act as a central repo for ConnectorFactory
 * objects.
 */

public class TransportConfigurationUtil {

   private static final Map<String, Map<String, Object>> DEFAULTS = new HashMap<>();

   private static final HashMap<String, Object> EMPTY_HELPER = new HashMap<>();

   public static Map<String, Object> getDefaults(String className) {
      if (className == null) {
         /* Returns a new clone of the empty helper.  This allows any parent objects to update the map key/values
            without polluting the EMPTY_HELPER map. */
         return (Map<String, Object>) EMPTY_HELPER.clone();
      }

      if (!DEFAULTS.containsKey(className)) {
         Object object = instantiateObject(className, TransportConfigurationHelper.class);
         if (object != null && object instanceof TransportConfigurationHelper) {

            DEFAULTS.put(className, ((TransportConfigurationHelper) object).getDefaults());
         } else {
            DEFAULTS.put(className, EMPTY_HELPER);
         }
      }

      /* We need to return a copy of the default Map.  This means the defaults parent is able to update the map without
      modifying the original */
      return cloneDefaults(DEFAULTS.get(className));
   }

   private static Object instantiateObject(final String className, final Class expectedType) {
      return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
         try {
            return ClassloadingUtil.newInstanceFromClassLoader(TransportConfigurationUtil.class, className, expectedType);
         } catch (IllegalStateException e) {
            return null;
         }
      });
   }

   private static Map<String, Object> cloneDefaults(Map<String, Object> defaults) {
      Map<String, Object> cloned = new HashMap<>();
      for (Map.Entry entry : defaults.entrySet()) {
         cloned.put((String) entry.getKey(), entry.getValue());
      }
      return cloned;
   }

   public static boolean isSameHost(TransportConfiguration tc1, TransportConfiguration tc2) {
      if (NettyConnectorFactory.class.getName().equals(tc1.getFactoryClassName())) {
         String host1 = tc1.getParams().get("host") != null ? tc1.getParams().get("host").toString() : TransportConstants.DEFAULT_HOST;
         String host2 = tc2.getParams().get("host") != null ? tc2.getParams().get("host").toString() : TransportConstants.DEFAULT_HOST;
         String port1 = String.valueOf(tc1.getParams().get("port") != null ? tc1.getParams().get("port") : TransportConstants.DEFAULT_PORT);
         String port2 = String.valueOf(tc2.getParams().get("port") != null ? tc2.getParams().get("port") : TransportConstants.DEFAULT_PORT);
         return host1.equals(host2) && port1.equals(port2);
      } else if ("org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory".equals(tc1.getFactoryClassName())) {
         String serverId1 = tc1.getParams().get("serverId") != null ? tc1.getParams().get("serverId").toString() : "0";
         String serverId2 = tc2.getParams().get("serverId") != null ? tc2.getParams().get("serverId").toString() : "0";
         return serverId1.equals(serverId2);
      }
      return false;
   }

}