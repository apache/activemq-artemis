/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.cluster.impl;

import org.apache.activemq.artemis.utils.uri.BeanSupport;
import org.apache.commons.beanutils.Converter;

public enum MessageLoadBalancingType {
   OFF("OFF"), STRICT("STRICT"), ON_DEMAND("ON_DEMAND");

   static {
      // for URI support on ClusterConnection
      BeanSupport.registerConverter(new MessageLoadBalancingTypeConverter(), MessageLoadBalancingType.class);
   }

   static class MessageLoadBalancingTypeConverter implements Converter {

      @Override
      public <T> T convert(Class<T> type, Object value) {
         return type.cast(MessageLoadBalancingType.getType(value.toString()));
      }
   }

   private String type;

   MessageLoadBalancingType(final String type) {
      this.type = type;
   }

   public String getType() {
      return type;
   }

   public static MessageLoadBalancingType getType(String string) {
      if (string.equals(OFF.getType())) {
         return MessageLoadBalancingType.OFF;
      } else if (string.equals(STRICT.getType())) {
         return MessageLoadBalancingType.STRICT;
      } else if (string.equals(ON_DEMAND.getType())) {
         return MessageLoadBalancingType.ON_DEMAND;
      } else {
         return null;
      }
   }
}
