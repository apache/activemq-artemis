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
import java.util.Map;

public class ConnectorServiceConfiguration implements Serializable {

   private static final long serialVersionUID = -641207073030767325L;

   private String name;

   private String factoryClassName;

   private Map<String, Object> params;

   public ConnectorServiceConfiguration() {
   }

   public String getConnectorName() {
      return name;
   }

   public String getFactoryClassName() {
      return factoryClassName;
   }

   public Map<String, Object> getParams() {
      return params;
   }

   public String getName() {
      return name;
   }

   public ConnectorServiceConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public ConnectorServiceConfiguration setFactoryClassName(String factoryClassName) {
      this.factoryClassName = factoryClassName;
      return this;
   }

   public ConnectorServiceConfiguration setParams(Map<String, Object> params) {
      this.params = params;
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      ConnectorServiceConfiguration that = (ConnectorServiceConfiguration) o;

      if (getFactoryClassName() != null ? !getFactoryClassName().equals(that.getFactoryClassName()) : that.getFactoryClassName() != null)
         return false;
      if (getConnectorName() != null ? !getConnectorName().equals(that.getConnectorName()) : that.getConnectorName() != null)
         return false;
      if (getParams() != null ? !getParams().equals(that.getParams()) : that.getParams() != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = getConnectorName() != null ? getConnectorName().hashCode() : 0;
      result = 31 * result + (getFactoryClassName() != null ? getFactoryClassName().hashCode() : 0);
      result = 31 * result + (getParams() != null ? getParams().hashCode() : 0);
      return result;
   }
}
