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
import java.io.StringReader;
import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.json.JsonString;
import org.apache.activemq.artemis.json.JsonValue;
import org.apache.activemq.artemis.utils.BufferHelper;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.JsonLoader;
import org.apache.activemq.artemis.utils.UUIDGenerator;

public class DivertConfiguration implements Serializable, EncodingSupport {

   private static final long serialVersionUID = 6910543740464269629L;

   public static String NAME = "name";
   public static String ROUTING_NAME = "routing-name";
   public static String ADDRESS = "address";
   public static String FORWARDING_ADDRESS = "forwarding-address";
   public static String EXCLUSIVE = "exclusive";
   public static String FILTER_STRING = "filter-string";
   public static String TRANSFORMER_CONFIGURATION = "transformer-configuration";
   public static String ROUTING_TYPE = "routing-type";

   private String name = null;

   private String routingName = UUIDGenerator.getInstance().generateStringUUID();

   private String address = null;

   private String forwardingAddress = null;

   private boolean exclusive = ActiveMQDefaultConfiguration.isDefaultDivertExclusive();

   private String filterString = null;

   private TransformerConfiguration transformerConfiguration = null;

   private ComponentConfigurationRoutingType routingType = ComponentConfigurationRoutingType.valueOf(ActiveMQDefaultConfiguration.getDefaultDivertRoutingType());

   public DivertConfiguration() {
   }

   /**
    * Set the value of a parameter based on its "key" {@code String}. Valid key names and corresponding {@code static}
    * {@code final} are:
    * <p><ul>
    * <li>name: {@link #NAME}
    * <li>routing-name: {@link #ROUTING_NAME}
    * <li>address: {@link #ADDRESS}
    * <li>forwarding-address: {@link #FORWARDING_ADDRESS}
    * <li>exclusive: {@link #EXCLUSIVE}
    * <li>filter-string: {@link #FILTER_STRING}
    * <li>transformer-configuration: {@link #TRANSFORMER_CONFIGURATION}
    * <li>routing-type: {@link #ROUTING_TYPE}
    * </ul><p>
    * The {@code String}-based values will be converted to the proper value types based on the underlying property. For
    * example, if you pass the value "TRUE" for the key "exclusive" the {@code String} "TRUE" will be converted to
    * the {@code Boolean} {@code true}.
    *
    * @param key the key to set to the value
    * @param value the value to set for the key
    * @return this {@code DivertConfiguration}
    */
   public DivertConfiguration set(String key, String value) {
      if (key != null) {
         if (key.equals(NAME)) {
            setName(value);
         } else if (key.equals(ROUTING_NAME)) {
            setRoutingName(value);
         } else if (key.equals(ADDRESS)) {
            setAddress(value);
         } else if (key.equals(FORWARDING_ADDRESS)) {
            setForwardingAddress(value);
         } else if (key.equals(EXCLUSIVE)) {
            setExclusive(Boolean.parseBoolean(value));
         } else if (key.equals(FILTER_STRING)) {
            setFilterString(value);
         } else if (key.equals(TRANSFORMER_CONFIGURATION)) {
            // create a transformer instance from a JSON string
            TransformerConfiguration transformerConfiguration = TransformerConfiguration.fromJSON(value);
            if (transformerConfiguration != null) {
               setTransformerConfiguration(transformerConfiguration);
            }
         } else if (key.equals(ROUTING_TYPE)) {
            setRoutingType(ComponentConfigurationRoutingType.valueOf(value));
         }
      }
      return this;
   }

   public String getName() {
      return name;
   }

   public String getRoutingName() {
      return routingName;
   }

   public String getAddress() {
      return address;
   }

   public String getForwardingAddress() {
      return forwardingAddress;
   }

   public boolean isExclusive() {
      return exclusive;
   }

   public String getFilterString() {
      return filterString;
   }

   public TransformerConfiguration getTransformerConfiguration() {
      return transformerConfiguration;
   }

   public ComponentConfigurationRoutingType getRoutingType() {
      return routingType;
   }

   /**
    * @param name the name to set
    */
   public DivertConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   /**
    * @param routingName the routingName to set
    */
   public DivertConfiguration setRoutingName(final String routingName) {
      if (routingName == null) {
         this.routingName = UUIDGenerator.getInstance().generateStringUUID();
      } else {
         this.routingName = routingName;
      }
      return this;
   }

   /**
    * @param address the address to set
    */
   public DivertConfiguration setAddress(final String address) {
      this.address = address;
      return this;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public DivertConfiguration setForwardingAddress(final String forwardingAddress) {
      this.forwardingAddress = forwardingAddress;
      return this;
   }

   /**
    * @param exclusive the exclusive to set
    */
   public DivertConfiguration setExclusive(final boolean exclusive) {
      this.exclusive = exclusive;
      return this;
   }

   /**
    * @param filterString the filterString to set
    */
   public DivertConfiguration setFilterString(final String filterString) {
      this.filterString = filterString;
      return this;
   }

   /**
    * @param transformerConfiguration the transformerConfiguration to set
    */
   public DivertConfiguration setTransformerConfiguration(final TransformerConfiguration transformerConfiguration) {
      this.transformerConfiguration = transformerConfiguration;
      return this;
   }

   /**
    * @param routingType the routingType to set
    */
   public DivertConfiguration setRoutingType(final ComponentConfigurationRoutingType routingType) {
      this.routingType = routingType;
      return this;
   }

   /**
    * This method returns a JSON-formatted {@code String} representation of this {@code DivertConfiguration}. It is a
    * simple collection of key/value pairs. The keys used are referenced in {@link #set(String, String)}.
    *
    * @return a JSON-formatted {@code String} representation of this {@code DivertConfiguration}
    */
   public String toJSON() {
      JsonObjectBuilder builder = JsonLoader.createObjectBuilder();

      if (getName() != null) {
         builder.add(NAME, getName());
      }
      if (getRoutingName() != null) {
         builder.add(ROUTING_NAME, getRoutingName());
      }
      if (getAddress() != null) {
         builder.add(ADDRESS, getAddress());
      }
      if (getForwardingAddress() != null) {
         builder.add(FORWARDING_ADDRESS, getForwardingAddress());
      }

      builder.add(EXCLUSIVE, isExclusive());

      if (getFilterString() != null) {
         builder.add(FILTER_STRING, getFilterString());
      }

      TransformerConfiguration tc = getTransformerConfiguration();
      if (tc != null) {
         builder.add(TRANSFORMER_CONFIGURATION, tc.createJsonObjectBuilder());
      }

      if (getRoutingType() != null) {
         builder.add(ROUTING_TYPE, getRoutingType().name());
      }

      return builder.build().toString();
   }

   /**
    * This method returns a {@code DivertConfiguration} created from the JSON-formatted input {@code String}. The input
    * should be a simple object of key/value pairs. Valid keys are referenced in {@link #set(String, String)}.
    *
    * @param jsonString json string
    * @return the {@code DivertConfiguration} created from the JSON-formatted input {@code String}
    */
   public static DivertConfiguration fromJSON(String jsonString) {
      JsonObject json = JsonLoader.readObject(new StringReader(jsonString));

      DivertConfiguration result = new DivertConfiguration();

      for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
         if (entry.getValue().getValueType() == JsonValue.ValueType.STRING) {
            result.set(entry.getKey(), ((JsonString) entry.getValue()).getString());
         } else if (entry.getValue().getValueType() == JsonValue.ValueType.NULL) {
            result.set(entry.getKey(), null);
         } else {
            result.set(entry.getKey(), entry.getValue().toString());
         }
      }

      return result;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (exclusive ? 1231 : 1237);
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((forwardingAddress == null) ? 0 : forwardingAddress.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((routingName == null) ? 0 : routingName.hashCode());
      result = prime * result + ((transformerConfiguration == null) ? 0 : transformerConfiguration.hashCode());
      result = prime * result + ((routingType == null) ? 0 : routingType.hashCode());
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
      DivertConfiguration other = (DivertConfiguration) obj;
      if (address == null) {
         if (other.address != null)
            return false;
      } else if (!address.equals(other.address))
         return false;
      if (exclusive != other.exclusive)
         return false;
      if (filterString == null) {
         if (other.filterString != null)
            return false;
      } else if (!filterString.equals(other.filterString))
         return false;
      if (forwardingAddress == null) {
         if (other.forwardingAddress != null)
            return false;
      } else if (!forwardingAddress.equals(other.forwardingAddress))
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      if (routingName == null) {
         if (other.routingName != null)
            return false;
      } else if (!routingName.equals(other.routingName))
         return false;
      if (transformerConfiguration == null) {
         if (other.transformerConfiguration != null)
            return false;
      } else if (!transformerConfiguration.equals(other.transformerConfiguration))
         return false;
      if (routingType == null) {
         if (other.routingType != null)
            return false;
      } else if (!routingType.equals(other.routingType))
         return false;
      return true;
   }


   @Override
   public int getEncodeSize() {
      int transformerSize;
      if (transformerConfiguration != null) {
         transformerSize = BufferHelper.sizeOfNullableString(transformerConfiguration.getClassName());
         transformerSize += DataConstants.SIZE_INT;
         for (Map.Entry<String, String> entry : transformerConfiguration.getProperties().entrySet()) {
            transformerSize += BufferHelper.sizeOfNullableString(entry.getKey());
            transformerSize += BufferHelper.sizeOfNullableString(entry.getValue());
         }
      } else {
         transformerSize = DataConstants.SIZE_NULL;
      }
      int size =  BufferHelper.sizeOfNullableString(name) +
            BufferHelper.sizeOfNullableString(address) +
            BufferHelper.sizeOfNullableString(forwardingAddress) +
            BufferHelper.sizeOfNullableString(routingName) +
            DataConstants.SIZE_BOOLEAN +
            BufferHelper.sizeOfNullableString(filterString) +
            DataConstants.SIZE_BYTE + transformerSize;
      return size;
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      buffer.writeNullableString(name);
      buffer.writeNullableString(address);
      buffer.writeNullableString(forwardingAddress);
      buffer.writeNullableString(routingName);
      buffer.writeBoolean(exclusive);
      buffer.writeNullableString(filterString);
      buffer.writeByte(routingType != null ? routingType.getType() : ComponentConfigurationRoutingType.valueOf(ActiveMQDefaultConfiguration.getDefaultDivertRoutingType()).getType());
      if (transformerConfiguration != null) {
         buffer.writeNullableString(transformerConfiguration.getClassName());
         Map<String, String> properties = transformerConfiguration.getProperties();
         buffer.writeInt(properties.size());
         for (Map.Entry<String, String> entry : properties.entrySet()) {
            buffer.writeNullableString(entry.getKey());
            buffer.writeNullableString(entry.getValue());
         }
      } else {
         buffer.writeNullableString(null);
      }
   }

   @Override
   public String toString() {
      return "DivertConfiguration [" +
         "name=" + name +
         ", routingName=" + routingName +
         ", address=" + address +
         ", forwardingAddress=" + forwardingAddress +
         ", exclusive=" + exclusive +
         ", filterString=" + filterString +
         ", routing-type=" + routingType +
         ", transformerConfiguration=" + transformerConfiguration + "]";
   }

   @Override
   public void decode(ActiveMQBuffer buffer) {
      name = buffer.readNullableString();
      address = buffer.readNullableString();
      forwardingAddress = buffer.readNullableString();
      routingName = buffer.readNullableString();
      exclusive = buffer.readBoolean();
      filterString = buffer.readNullableString();
      routingType = ComponentConfigurationRoutingType.getType(buffer.readByte());
      String transformerClassName = buffer.readNullableString();
      if (transformerClassName != null) {
         transformerConfiguration = new TransformerConfiguration(transformerClassName);
         int propsSize = buffer.readInt();
         for (int i = 0; i < propsSize; i++) {
            transformerConfiguration.getProperties().put(buffer.readNullableString(), buffer.readNullableString());
         }
      }
   }
}
