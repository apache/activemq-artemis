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
package org.apache.activemq.artemis.core.config.federation;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.config.TransformerConfiguration;
import org.apache.activemq.artemis.utils.Preconditions;

public class FederationTransformerConfiguration implements Serializable {

   private String name;

   private TransformerConfiguration transformerConfiguration;

   public FederationTransformerConfiguration() {
   }

   public FederationTransformerConfiguration(String name, TransformerConfiguration transformerConfiguration) {
      this.name = name;
      this.transformerConfiguration = transformerConfiguration;
   }

   public String getName() {
      return name;
   }

   public TransformerConfiguration getTransformerConfiguration() {
      return transformerConfiguration;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederationTransformerConfiguration)) return false;
      FederationTransformerConfiguration that = (FederationTransformerConfiguration) o;
      return Objects.equals(name, that.name) &&
            Objects.equals(transformerConfiguration, that.transformerConfiguration);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, transformerConfiguration);
   }


   public void encode(ActiveMQBuffer buffer) {
      Preconditions.checkArgument(name != null, "name can not be null");
      Preconditions.checkArgument(transformerConfiguration != null, "transformerConfiguration can not be null");
      buffer.writeString(name);

      buffer.writeString(transformerConfiguration.getClassName());
      buffer.writeInt(transformerConfiguration.getProperties() == null ? 0 : transformerConfiguration.getProperties().size());
      if (transformerConfiguration.getProperties() != null) {
         for (Map.Entry<String, String> entry : transformerConfiguration.getProperties().entrySet()) {
            buffer.writeString(entry.getKey());
            buffer.writeString(entry.getValue());
         }
      }
   }

   public void decode(ActiveMQBuffer buffer) {
      name = buffer.readString();
      transformerConfiguration = new TransformerConfiguration(buffer.readString());

      final int propertiesSize = buffer.readInt();
      for (int i = 0; i < propertiesSize; i++) {
         transformerConfiguration.getProperties().put(buffer.readString(), buffer.readString());
      }
   }
}
