/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.management.impl.openmbean;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;

public final class OpenTypeSupport {

   private static MessageOpenTypeFactory TEXT_FACTORY = new TextMessageOpenTypeFactory();
   private static MessageOpenTypeFactory BYTES_FACTORY = new BytesMessageOpenTypeFactory();

   private OpenTypeSupport() {
   }

   public static CompositeData convert(MessageReference ref, int valueSizeLimit) throws OpenDataException {
      CompositeType ct;

      ICoreMessage message = ref.getMessage().toCore();

      Map<String, Object> fields;
      byte type = message.getType();

      switch(type) {
         case Message.TEXT_TYPE:
            ct = TEXT_FACTORY.getCompositeType();
            fields = TEXT_FACTORY.getFields(ref, valueSizeLimit);
            break;
         default:
            ct = BYTES_FACTORY.getCompositeType();
            fields = BYTES_FACTORY.getFields(ref, valueSizeLimit);
            break;
      }
      return new CompositeDataSupport(ct, fields);
   }

   static class MessageOpenTypeFactory {

      private CompositeType compositeType;
      private final List<String> itemNamesList = new ArrayList<>();
      private final List<String> itemDescriptionsList = new ArrayList<>();
      private final List<OpenType> itemTypesList = new ArrayList<>();

      protected TabularType stringPropertyTabularType;
      protected TabularType booleanPropertyTabularType;
      protected TabularType bytePropertyTabularType;
      protected TabularType shortPropertyTabularType;
      protected TabularType intPropertyTabularType;
      protected TabularType longPropertyTabularType;
      protected TabularType floatPropertyTabularType;
      protected TabularType doublePropertyTabularType;
      protected Object[][] typedPropertyFields;

      protected String getTypeName() {
         return Message.class.getName();
      }

      public CompositeType getCompositeType() throws OpenDataException {
         if (compositeType == null) {
            init();
            compositeType = createCompositeType();
         }
         return compositeType;
      }

      protected void init() throws OpenDataException {

         addItem(CompositeDataConstants.ADDRESS, CompositeDataConstants.ADDRESS_DESCRIPTION, SimpleType.STRING);
         addItem(CompositeDataConstants.MESSAGE_ID, CompositeDataConstants.MESSAGE_ID_DESCRIPTION, SimpleType.STRING);
         addItem(CompositeDataConstants.USER_ID, CompositeDataConstants.USER_ID_DESCRIPTION, SimpleType.STRING);
         addItem(CompositeDataConstants.TYPE, CompositeDataConstants.TYPE_DESCRIPTION, SimpleType.BYTE);
         addItem(CompositeDataConstants.DURABLE, CompositeDataConstants.DURABLE_DESCRIPTION, SimpleType.BOOLEAN);
         addItem(CompositeDataConstants.EXPIRATION, CompositeDataConstants.EXPIRATION_DESCRIPTION, SimpleType.LONG);
         addItem(CompositeDataConstants.PRIORITY, CompositeDataConstants.PRIORITY_DESCRIPTION, SimpleType.BYTE);
         addItem(CompositeDataConstants.REDELIVERED, CompositeDataConstants.REDELIVERED_DESCRIPTION, SimpleType.BOOLEAN);
         addItem(CompositeDataConstants.TIMESTAMP, CompositeDataConstants.TIMESTAMP_DESCRIPTION, SimpleType.LONG);
         addItem(CompositeDataConstants.LARGE_MESSAGE, CompositeDataConstants.LARGE_MESSAGE_DESCRIPTION, SimpleType.BOOLEAN);
         addItem(CompositeDataConstants.PERSISTENT_SIZE, CompositeDataConstants.PERSISTENT_SIZE_DESCRIPTION, SimpleType.LONG);

         addItem(CompositeDataConstants.PROPERTIES, CompositeDataConstants.PROPERTIES_DESCRIPTION, SimpleType.STRING);

         // now lets expose the type safe properties
         stringPropertyTabularType = createTabularType(String.class, SimpleType.STRING);
         booleanPropertyTabularType = createTabularType(Boolean.class, SimpleType.BOOLEAN);
         bytePropertyTabularType = createTabularType(Byte.class, SimpleType.BYTE);
         shortPropertyTabularType = createTabularType(Short.class, SimpleType.SHORT);
         intPropertyTabularType = createTabularType(Integer.class, SimpleType.INTEGER);
         longPropertyTabularType = createTabularType(Long.class, SimpleType.LONG);
         floatPropertyTabularType = createTabularType(Float.class, SimpleType.FLOAT);
         doublePropertyTabularType = createTabularType(Double.class, SimpleType.DOUBLE);

         addItem(CompositeDataConstants.STRING_PROPERTIES, CompositeDataConstants.STRING_PROPERTIES_DESCRIPTION, stringPropertyTabularType);
         addItem(CompositeDataConstants.BOOLEAN_PROPERTIES, CompositeDataConstants.BOOLEAN_PROPERTIES_DESCRIPTION, booleanPropertyTabularType);
         addItem(CompositeDataConstants.BYTE_PROPERTIES, CompositeDataConstants.BYTE_PROPERTIES_DESCRIPTION, bytePropertyTabularType);
         addItem(CompositeDataConstants.SHORT_PROPERTIES, CompositeDataConstants.SHORT_PROPERTIES_DESCRIPTION, shortPropertyTabularType);
         addItem(CompositeDataConstants.INT_PROPERTIES, CompositeDataConstants.INT_PROPERTIES_DESCRIPTION, intPropertyTabularType);
         addItem(CompositeDataConstants.LONG_PROPERTIES, CompositeDataConstants.LONG_PROPERTIES_DESCRIPTION, longPropertyTabularType);
         addItem(CompositeDataConstants.FLOAT_PROPERTIES, CompositeDataConstants.FLOAT_PROPERTIES_DESCRIPTION, floatPropertyTabularType);
         addItem(CompositeDataConstants.DOUBLE_PROPERTIES, CompositeDataConstants.DOUBLE_PROPERTIES_DESCRIPTION, doublePropertyTabularType);

         typedPropertyFields = new Object[][] {
            {CompositeDataConstants.STRING_PROPERTIES, stringPropertyTabularType, String.class},
            {CompositeDataConstants.BOOLEAN_PROPERTIES, booleanPropertyTabularType, Boolean.class},
            {CompositeDataConstants.BYTE_PROPERTIES, bytePropertyTabularType, Byte.class},
            {CompositeDataConstants.SHORT_PROPERTIES, shortPropertyTabularType, Short.class},
            {CompositeDataConstants.INT_PROPERTIES, intPropertyTabularType, Integer.class},
            {CompositeDataConstants.LONG_PROPERTIES, longPropertyTabularType, Long.class},
            {CompositeDataConstants.FLOAT_PROPERTIES, floatPropertyTabularType, Float.class},
            {CompositeDataConstants.DOUBLE_PROPERTIES, doublePropertyTabularType, Double.class}
         };

      }

      public Map<String, Object> getFields(MessageReference ref, int valueSizeLimit) throws OpenDataException {
         Map<String, Object> rc = new HashMap<>();
         ICoreMessage m = ref.getMessage().toCore();
         rc.put(CompositeDataConstants.MESSAGE_ID, "" + m.getMessageID());
         if (m.getUserID() != null) {
            rc.put(CompositeDataConstants.USER_ID, "ID:" + m.getUserID().toString());
         } else {
            rc.put(CompositeDataConstants.USER_ID, "");
         }
         rc.put(CompositeDataConstants.ADDRESS, m.getAddress() == null ? "" : m.getAddress().toString());
         rc.put(CompositeDataConstants.TYPE, m.getType());
         rc.put(CompositeDataConstants.DURABLE, m.isDurable());
         rc.put(CompositeDataConstants.EXPIRATION, m.getExpiration());
         rc.put(CompositeDataConstants.TIMESTAMP, m.getTimestamp());
         rc.put(CompositeDataConstants.PRIORITY, m.getPriority());
         rc.put(CompositeDataConstants.REDELIVERED, ref.getDeliveryCount() > 1);
         rc.put(CompositeDataConstants.LARGE_MESSAGE, m.isLargeMessage());
         try {
            rc.put(CompositeDataConstants.PERSISTENT_SIZE, m.getPersistentSize());
         } catch (final ActiveMQException e1) {
            rc.put(CompositeDataConstants.PERSISTENT_SIZE, -1);
         }

         Map<String, Object> propertyMap = m.toPropertyMap(valueSizeLimit);

         rc.put(CompositeDataConstants.PROPERTIES, JsonUtil.truncate("" + propertyMap, valueSizeLimit));

         // only populate if there are some values
         TabularDataSupport tabularData;
         for (Object[] typedPropertyInfo : typedPropertyFields) {
            tabularData = null;
            try {
               tabularData = createTabularData(propertyMap, (TabularType) typedPropertyInfo[1], (Class) typedPropertyInfo[2]);
            } catch (Exception ignored) {
            }
            if (tabularData != null && !tabularData.isEmpty()) {
               rc.put((String) typedPropertyInfo[0], tabularData);
            } else {
               rc.put((String) typedPropertyInfo[0], null);
            }
         }
         return rc;
      }

      protected String toString(Object value) {
         if (value == null) {
            return null;
         }
         return value.toString();
      }

      protected CompositeType createCompositeType() throws OpenDataException {
         String[] itemNames = itemNamesList.toArray(new String[itemNamesList.size()]);
         String[] itemDescriptions = itemDescriptionsList.toArray(new String[itemDescriptionsList.size()]);
         OpenType[] itemTypes = itemTypesList.toArray(new OpenType[itemTypesList.size()]);
         return new CompositeType(getTypeName(), getDescription(), itemNames, itemDescriptions, itemTypes);
      }

      protected String getDescription() {
         return getTypeName();
      }

      protected <T> TabularType createTabularType(Class<T> type, OpenType openType) throws OpenDataException {
         String typeName = "java.util.Map<java.lang.String, " + type.getName() + ">";
         String[] keyValue = new String[]{"key", "value"};
         OpenType[] openTypes = new OpenType[]{SimpleType.STRING, openType};
         CompositeType rowType = new CompositeType(typeName, typeName, keyValue, keyValue, openTypes);
         return new TabularType(typeName, typeName, rowType, new String[]{"key"});
      }

      protected TabularDataSupport createTabularData(Map<String, Object> entries,
                                                     TabularType type,
                                                     Class valueType) throws IOException, OpenDataException {
         TabularDataSupport answer = new TabularDataSupport(type);

         for (String key : entries.keySet()) {
            Object value = entries.get(key);
            if (valueType.isInstance(value)) {
               CompositeDataSupport compositeData = createTabularRowValue(type, key, value);
               answer.put(compositeData);
            } else if (valueType == String.class && value instanceof SimpleString) {
               CompositeDataSupport compositeData = createTabularRowValue(type, key, value.toString());
               answer.put(compositeData);
            }
         }
         return answer;
      }

      protected CompositeDataSupport createTabularRowValue(TabularType type,
                                                           String key,
                                                           Object value) throws OpenDataException {
         Map<String, Object> fields = new HashMap<>();
         fields.put("key", key);
         fields.put("value", value);
         return new CompositeDataSupport(type.getRowType(), fields);
      }

      protected void addItem(String name, String description, OpenType type) {
         itemNamesList.add(name);
         itemDescriptionsList.add(description);
         itemTypesList.add(type);
      }
   }


   static class BytesMessageOpenTypeFactory extends MessageOpenTypeFactory {
      protected ArrayType body;

      @Override
      protected void init() throws OpenDataException {
         super.init();
         body = new ArrayType(SimpleType.BYTE, true);
         addItem(CompositeDataConstants.BODY, CompositeDataConstants.BODY_DESCRIPTION, body);
      }

      @Override
      public Map<String, Object> getFields(MessageReference ref, int valueSizeLimit) throws OpenDataException {
         Map<String, Object> rc = super.getFields(ref, valueSizeLimit);
         ICoreMessage m = ref.getMessage().toCore();
         if (!m.isLargeMessage()) {
            ActiveMQBuffer bodyCopy = m.getReadOnlyBodyBuffer();
            byte[] bytes = new byte[bodyCopy.readableBytes() <= valueSizeLimit ? bodyCopy.readableBytes() : valueSizeLimit + 1];
            bodyCopy.readBytes(bytes);
            rc.put(CompositeDataConstants.BODY, JsonUtil.truncate(bytes, valueSizeLimit));
         } else {
            rc.put(CompositeDataConstants.BODY, new byte[0]);
         }
         return rc;
      }
   }

   static class TextMessageOpenTypeFactory extends MessageOpenTypeFactory {
      protected SimpleType text;

      @Override
      protected void init() throws OpenDataException {
         super.init();
         addItem(CompositeDataConstants.TEXT_BODY, CompositeDataConstants.TEXT_BODY, SimpleType.STRING);
      }

      @Override
      public Map<String, Object> getFields(MessageReference ref, int valueSizeLimit) throws OpenDataException {
         Map<String, Object> rc = super.getFields(ref, valueSizeLimit);
         ICoreMessage m = ref.getMessage().toCore();
         if (!m.isLargeMessage()) {
            if (m.containsProperty(Message.HDR_LARGE_COMPRESSED)) {
               rc.put(CompositeDataConstants.TEXT_BODY, "[compressed]");
            } else {
               final String text = m.getReadOnlyBodyBuffer().readString();
               rc.put(CompositeDataConstants.TEXT_BODY, text != null ? JsonUtil.truncate(text, valueSizeLimit) : "");
            }
         } else {
            rc.put(CompositeDataConstants.TEXT_BODY, "[large message]");
         }
         return rc;
      }
   }
}
