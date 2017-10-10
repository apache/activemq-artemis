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
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class TransformerConfiguration implements Serializable {

   private static final long serialVersionUID = -1057244274380572226L;

   private final String className;

   private Map<String, String> properties = new HashMap<>();

   public TransformerConfiguration(String className) {
      this.className = className;
   }

   public String getClassName() {
      return className;
   }

   public Map<String, String> getProperties() {
      return properties;
   }

   /**
    * @param properties the properties to set
    */
   public TransformerConfiguration setProperties(final Map<String, String> properties) {
      if (properties != null) {
         this.properties.putAll(properties);
      }
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((className == null) ? 0 : className.hashCode());
      result = prime * result + ((properties == null) ? 0 : properties.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TransformerConfiguration other = (TransformerConfiguration) obj;
      if (className == null) {
         if (other.className != null)
            return false;
      } else if (!className.equals(other.className))
         return false;
      if (properties == null) {
         if (other.properties != null)
            return false;
      } else if (!properties.equals(other.properties))
         return false;
      return true;
   }

}
