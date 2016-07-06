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
package org.apache.activemq.artemis.jms.management.impl.openmbean;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.management.impl.openmbean.CompositeDataConstants;
import org.apache.activemq.artemis.reader.MapMessageUtil;
import org.apache.activemq.artemis.utils.TypedProperties;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JMSOpenTypeSupport {

   public interface OpenTypeFactory {
      CompositeType getCompositeType() throws OpenDataException;

      Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException;
   }

   private static final Map<Byte, AbstractOpenTypeFactory> OPEN_TYPE_FACTORIES = new HashMap<>();

   public abstract static class AbstractOpenTypeFactory implements OpenTypeFactory {

      private CompositeType compositeType;
      private final List<String> itemNamesList = new ArrayList<>();
      private final List<String> itemDescriptionsList = new ArrayList<>();
      private final List<OpenType> itemTypesList = new ArrayList<>();

      @Override
      public CompositeType getCompositeType() throws OpenDataException {
         if (compositeType == null) {
            init();
            compositeType = createCompositeType();
         }
         return compositeType;
      }

      protected void init() throws OpenDataException {
      }

      protected CompositeType createCompositeType() throws OpenDataException {
         String[] itemNames = itemNamesList.toArray(new String[itemNamesList.size()]);
         String[] itemDescriptions = itemDescriptionsList.toArray(new String[itemDescriptionsList.size()]);
         OpenType[] itemTypes = itemTypesList.toArray(new OpenType[itemTypesList.size()]);
         return new CompositeType(getTypeName(), getDescription(), itemNames, itemDescriptions, itemTypes);
      }

      protected abstract String getTypeName();

      protected void addItem(String name, String description, OpenType type) {
         itemNamesList.add(name);
         itemDescriptionsList.add(description);
         itemTypesList.add(type);
      }

      protected String getDescription() {
         return getTypeName();
      }

      @Override
      public Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException {
         Map<String, Object> rc = new HashMap<>();
         return rc;
      }
   }

   static class MessageOpenTypeFactory extends AbstractOpenTypeFactory {
      protected TabularType stringPropertyTabularType;
      protected TabularType booleanPropertyTabularType;
      protected TabularType bytePropertyTabularType;
      protected TabularType shortPropertyTabularType;
      protected TabularType intPropertyTabularType;
      protected TabularType longPropertyTabularType;
      protected TabularType floatPropertyTabularType;
      protected TabularType doublePropertyTabularType;

      protected ArrayType body;

      @Override
      protected String getTypeName() {
         return Message.class.getName();
      }

      @Override
      protected void init() throws OpenDataException {
         super.init();

         addItem(JMSCompositeDataConstants.JMS_DESTINATION, JMSCompositeDataConstants.JMS_DESTINATION_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_MESSAGE_ID, JMSCompositeDataConstants.JMS_MESSAGE_ID_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_CORRELATION_ID, JMSCompositeDataConstants.JMS_CORRELATION_ID_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_TYPE, JMSCompositeDataConstants.JMS_TYPE_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_DELIVERY_MODE, JMSCompositeDataConstants.JMS_DELIVERY_MODE_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_EXPIRATION, JMSCompositeDataConstants.JMS_EXPIRATION_DESCRIPTION, SimpleType.LONG);
         addItem(JMSCompositeDataConstants.JMS_PRIORITY, JMSCompositeDataConstants.JMS_PRIORITY_DESCRIPTION, SimpleType.INTEGER);
         addItem(JMSCompositeDataConstants.JMS_REDELIVERED, JMSCompositeDataConstants.JMS_REDELIVERED_DESCRIPTION, SimpleType.BOOLEAN);
         addItem(JMSCompositeDataConstants.JMS_TIMESTAMP, JMSCompositeDataConstants.JMS_TIMESTAMP_DESCRIPTION, SimpleType.DATE);
         addItem(JMSCompositeDataConstants.JMSXGROUP_ID, JMSCompositeDataConstants.JMSXGROUP_ID_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMSXGROUP_SEQ, JMSCompositeDataConstants.JMSXGROUP_SEQ_DESCRIPTION, SimpleType.INTEGER);
         addItem(JMSCompositeDataConstants.JMSXUSER_ID, JMSCompositeDataConstants.JMSXUSER_ID_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.JMS_REPLY_TO, JMSCompositeDataConstants.JMS_REPLY_TO_DESCRIPTION, SimpleType.STRING);
         addItem(JMSCompositeDataConstants.ORIGINAL_DESTINATION, JMSCompositeDataConstants.ORIGINAL_DESTINATION_DESCRIPTION, SimpleType.STRING);
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
      }

      @Override
      public Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException {
         Map<String, Object> rc = super.getFields(data);
         putString(rc, data, JMSCompositeDataConstants.JMS_MESSAGE_ID, CompositeDataConstants.USER_ID);
         putString(rc, data, JMSCompositeDataConstants.JMS_DESTINATION, CompositeDataConstants.ADDRESS);
         putStringProperty(rc, data, JMSCompositeDataConstants.JMS_REPLY_TO, "JMSReplyTo");
         rc.put(JMSCompositeDataConstants.JMS_TYPE, getType());
         rc.put(JMSCompositeDataConstants.JMS_DELIVERY_MODE, ((Boolean)data.get(CompositeDataConstants.DURABLE)) ? "PERSISTENT" : "NON-PERSISTENT");
         rc.put(JMSCompositeDataConstants.JMS_EXPIRATION, data.get(CompositeDataConstants.EXPIRATION));
         rc.put(JMSCompositeDataConstants.JMS_TIMESTAMP, new Date((Long) data.get(CompositeDataConstants.TIMESTAMP)));
         rc.put(JMSCompositeDataConstants.JMS_PRIORITY, ((Byte) data.get(CompositeDataConstants.PRIORITY)).intValue());
         putStringProperty(rc, data, JMSCompositeDataConstants.JMS_CORRELATION_ID, JMSCompositeDataConstants.JMS_CORRELATION_ID);
         rc.put(JMSCompositeDataConstants.JMS_REDELIVERED, data.get(CompositeDataConstants.REDELIVERED));
         putStringProperty(rc, data, JMSCompositeDataConstants.JMSXGROUP_ID, Message.HDR_GROUP_ID.toString());
         putIntProperty(rc, data, JMSCompositeDataConstants.JMSXGROUP_SEQ, JMSCompositeDataConstants.JMSXGROUP_SEQ);
         putStringProperty(rc, data, JMSCompositeDataConstants.JMSXUSER_ID, Message.HDR_VALIDATED_USER.toString());
         putStringProperty(rc, data, JMSCompositeDataConstants.ORIGINAL_DESTINATION, Message.HDR_ORIGINAL_ADDRESS.toString());

         rc.put(CompositeDataConstants.PROPERTIES, "" + data.get(CompositeDataConstants.PROPERTIES));

         rc.put(CompositeDataConstants.STRING_PROPERTIES, data.get(CompositeDataConstants.STRING_PROPERTIES));
         rc.put(CompositeDataConstants.BOOLEAN_PROPERTIES, data.get(CompositeDataConstants.BOOLEAN_PROPERTIES));
         rc.put(CompositeDataConstants.BYTE_PROPERTIES, data.get(CompositeDataConstants.BYTE_PROPERTIES));
         rc.put(CompositeDataConstants.SHORT_PROPERTIES, data.get(CompositeDataConstants.SHORT_PROPERTIES));
         rc.put(CompositeDataConstants.INT_PROPERTIES, data.get(CompositeDataConstants.INT_PROPERTIES));
         rc.put(CompositeDataConstants.LONG_PROPERTIES, data.get(CompositeDataConstants.LONG_PROPERTIES));
         rc.put(CompositeDataConstants.FLOAT_PROPERTIES, data.get(CompositeDataConstants.FLOAT_PROPERTIES));
         rc.put(CompositeDataConstants.DOUBLE_PROPERTIES, data.get(CompositeDataConstants.DOUBLE_PROPERTIES));

         return rc;
      }

      private void putString(Map<String, Object> rc, CompositeDataSupport data, String target, String source) {
         String prop = (String) data.get(source);
         if (prop != null) {
            rc.put(target, prop);
         }
         else {
            rc.put(target, "");
         }
      }

      private void putStringProperty(Map<String, Object> rc, CompositeDataSupport data, String target, String source) {
         TabularDataSupport properties = (TabularDataSupport) data.get(CompositeDataConstants.STRING_PROPERTIES);
         Object[] keys = new Object[]{source};
         CompositeDataSupport cds = (CompositeDataSupport) properties.get(keys);
         String prop = "";
         if (cds != null && cds.get("value") != null) {
            prop = (String) cds.get("value");
         }
         rc.put(target, prop);
      }

      private void putIntProperty(Map<String, Object> rc, CompositeDataSupport data, String target, String source) {
         TabularDataSupport properties = (TabularDataSupport) data.get(CompositeDataConstants.INT_PROPERTIES);
         Object[] keys = new Object[]{source};
         CompositeDataSupport cds = (CompositeDataSupport) properties.get(keys);
         Integer prop = 0;
         if (cds != null && cds.get("value") != null) {
            prop = (Integer) cds.get("value");
         }
         rc.put(target, prop);
      }

      private String getType() {
         return "Message";
      }

      protected String toString(Object value) {
         if (value == null) {
            return null;
         }
         return value.toString();
      }


      protected <T> TabularType createTabularType(Class<T> type, OpenType openType) throws OpenDataException {
         String typeName = "java.util.Map<java.lang.String, " + type.getName() + ">";
         String[] keyValue = new String[]{"key", "value"};
         OpenType[] openTypes = new OpenType[]{SimpleType.STRING, openType};
         CompositeType rowType = new CompositeType(typeName, typeName, keyValue, keyValue, openTypes);
         return new TabularType(typeName, typeName, rowType, new String[]{"key"});
      }
   }

   static class ByteMessageOpenTypeFactory extends MessageOpenTypeFactory {


      @Override
      protected String getTypeName() {
         return "BytesMessage";
      }

      @Override
      protected void init() throws OpenDataException {
         super.init();
         addItem(JMSCompositeDataConstants.BODY_LENGTH, "Body length", SimpleType.LONG);
         addItem(JMSCompositeDataConstants.BODY_PREVIEW, "Body preview", new ArrayType(SimpleType.BYTE, true));
      }

      @Override
      public Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException {
         Map<String, Object> rc = super.getFields(data);
         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer((byte[]) data.get("body"));
         long length = 0;
         length = buffer.readableBytes();
         rc.put(JMSCompositeDataConstants.BODY_LENGTH, Long.valueOf(length));
         byte[] preview = new byte[(int) Math.min(length, 255)];
         buffer.readBytes(preview);
         rc.put(JMSCompositeDataConstants.BODY_PREVIEW, preview);
         return rc;
      }
   }

   static class MapMessageOpenTypeFactory extends MessageOpenTypeFactory {

      @Override
      protected String getTypeName() {
         return "MapMessage";
      }

      @Override
      protected void init() throws OpenDataException {
         super.init();
         addItem(JMSCompositeDataConstants.CONTENT_MAP, "Content map", SimpleType.STRING);
      }

      @Override
      public Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException {
         Map<String, Object> rc = super.getFields(data);
         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer((byte[]) data.get("body"));
         TypedProperties properties = new TypedProperties();
         MapMessageUtil.readBodyMap(buffer, properties);
         rc.put(JMSCompositeDataConstants.CONTENT_MAP, "" + properties.getMap());
         return rc;
      }
   }

   static class ObjectMessageOpenTypeFactory extends MessageOpenTypeFactory {
      @Override
      protected String getTypeName() {
         return "ObjectMessage";
      }
   }
   static class StreamMessageOpenTypeFactory extends MessageOpenTypeFactory {
      @Override
      protected String getTypeName() {
         return "StreamMessage";
      }
   }

   static class TextMessageOpenTypeFactory extends MessageOpenTypeFactory {

      @Override
      protected String getTypeName() {
         return "TextMessage";
      }

      @Override
      protected void init() throws OpenDataException {
         super.init();
         addItem(JMSCompositeDataConstants.MESSAGE_TEXT, JMSCompositeDataConstants.MESSAGE_TEXT, SimpleType.STRING);
      }

      @Override
      public Map<String, Object> getFields(CompositeDataSupport data) throws OpenDataException {
         Map<String, Object> rc = super.getFields(data);
         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer((byte[]) data.get("body"));
         SimpleString value = buffer.readNullableSimpleString();
         rc.put(JMSCompositeDataConstants.MESSAGE_TEXT, value != null ? value.toString() : "");
         return rc;
      }

   }

   static {
      OPEN_TYPE_FACTORIES.put(Message.DEFAULT_TYPE, new MessageOpenTypeFactory());
      OPEN_TYPE_FACTORIES.put(Message.TEXT_TYPE, new TextMessageOpenTypeFactory());
      OPEN_TYPE_FACTORIES.put(Message.BYTES_TYPE, new ByteMessageOpenTypeFactory());
      OPEN_TYPE_FACTORIES.put(Message.MAP_TYPE, new MapMessageOpenTypeFactory());
      OPEN_TYPE_FACTORIES.put(Message.OBJECT_TYPE, new ObjectMessageOpenTypeFactory());
      OPEN_TYPE_FACTORIES.put(Message.STREAM_TYPE, new StreamMessageOpenTypeFactory());
   }

   private JMSOpenTypeSupport() {
   }

   public static OpenTypeFactory getFactory(Byte type) throws OpenDataException {
      return OPEN_TYPE_FACTORIES.get(type);
   }

   public static CompositeData convert(CompositeDataSupport data) throws OpenDataException {
      OpenTypeFactory f = getFactory((Byte) data.get("type"));
      if (f == null) {
         throw new OpenDataException("Cannot create a CompositeData for type: " + data.get("type"));
      }
      CompositeType ct = f.getCompositeType();
      Map<String, Object> fields = f.getFields(data);
      return new CompositeDataSupport(ct, fields);
   }

}
