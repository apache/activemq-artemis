/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.utils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQPropertyConversionException;
import org.apache.activemq6.api.core.SimpleString;

import static org.apache.activemq6.utils.DataConstants.BOOLEAN;
import static org.apache.activemq6.utils.DataConstants.BYTE;
import static org.apache.activemq6.utils.DataConstants.BYTES;
import static org.apache.activemq6.utils.DataConstants.CHAR;
import static org.apache.activemq6.utils.DataConstants.DOUBLE;
import static org.apache.activemq6.utils.DataConstants.FLOAT;
import static org.apache.activemq6.utils.DataConstants.INT;
import static org.apache.activemq6.utils.DataConstants.LONG;
import static org.apache.activemq6.utils.DataConstants.NULL;
import static org.apache.activemq6.utils.DataConstants.SHORT;
import static org.apache.activemq6.utils.DataConstants.STRING;

/**
 * Property Value Conversion.
 * <p>
 * This implementation follows section 3.5.4 of the <i>Java Message Service</i> specification
 * (Version 1.1 April 12, 2002).
 * <p>
 * TODO - should have typed property getters and do conversions herein
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public final class TypedProperties
{

   private static final SimpleString HQ_PROPNAME = new SimpleString("_HQ_");

   private Map<SimpleString, PropertyValue> properties;

   private volatile int size;

   private boolean internalProperties;

   public TypedProperties()
   {
   }

   public int getMemoryOffset()
   {
      // The estimate is basically the encode size + 2 object references for each entry in the map
      // Note we don't include the attributes or anything else since they already included in the memory estimate
      // of the ServerMessage

      return properties == null ? 0 : size + 2 * DataConstants.SIZE_INT * properties.size();
   }

   public TypedProperties(final TypedProperties other)
   {
      properties = other.properties == null ? null : new HashMap<SimpleString, PropertyValue>(other.properties);
      size = other.size;
   }

   public boolean hasInternalProperties()
   {
      return internalProperties;
   }

   public void putBooleanProperty(final SimpleString key, final boolean value)
   {
      checkCreateProperties();
      doPutValue(key, new BooleanValue(value));
   }

   public void putByteProperty(final SimpleString key, final byte value)
   {
      checkCreateProperties();
      doPutValue(key, new ByteValue(value));
   }

   public void putBytesProperty(final SimpleString key, final byte[] value)
   {
      checkCreateProperties();
      doPutValue(key, value == null ? new NullValue() : new BytesValue(value));
   }

   public void putShortProperty(final SimpleString key, final short value)
   {
      checkCreateProperties();
      doPutValue(key, new ShortValue(value));
   }

   public void putIntProperty(final SimpleString key, final int value)
   {
      checkCreateProperties();
      doPutValue(key, new IntValue(value));
   }

   public void putLongProperty(final SimpleString key, final long value)
   {
      checkCreateProperties();
      doPutValue(key, new LongValue(value));
   }

   public void putFloatProperty(final SimpleString key, final float value)
   {
      checkCreateProperties();
      doPutValue(key, new FloatValue(value));
   }

   public void putDoubleProperty(final SimpleString key, final double value)
   {
      checkCreateProperties();
      doPutValue(key, new DoubleValue(value));
   }

   public void putSimpleStringProperty(final SimpleString key, final SimpleString value)
   {
      checkCreateProperties();
      doPutValue(key, value == null ? new NullValue() : new StringValue(value));
   }

   public void putNullValue(final SimpleString key)
   {
      checkCreateProperties();
      doPutValue(key, new NullValue());
   }

   public void putCharProperty(final SimpleString key, final char value)
   {
      checkCreateProperties();
      doPutValue(key, new CharValue(value));
   }

   public void putTypedProperties(final TypedProperties otherProps)
   {
      if (otherProps == null || otherProps.properties == null)
      {
         return;
      }

      checkCreateProperties();
      Set<Entry<SimpleString, PropertyValue>> otherEntries = otherProps.properties.entrySet();
      for (Entry<SimpleString, PropertyValue> otherEntry : otherEntries)
      {
         doPutValue(otherEntry.getKey(), otherEntry.getValue());
      }
   }

   public Object getProperty(final SimpleString key)
   {
      return doGetProperty(key);
   }

   public Boolean getBooleanProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Boolean.valueOf(null);
      }
      else if (value instanceof Boolean)
      {
         return (Boolean) value;
      }
      else if (value instanceof SimpleString)
      {
         return Boolean.valueOf(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Byte getByteProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Byte.valueOf(null);
      }
      else if (value instanceof Byte)
      {
         return (Byte) value;
      }
      else if (value instanceof SimpleString)
      {
         return Byte.parseByte(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Character getCharProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         throw new NullPointerException("Invalid conversion");
      }

      if (value instanceof Character)
      {
         return ((Character) value);
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public byte[] getBytesProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return null;
      }
      else if (value instanceof byte[])
      {
         return (byte[]) value;
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Double getDoubleProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Double.valueOf(null);
      }
      else if (value instanceof Float)
      {
         return ((Float) value).doubleValue();
      }
      else if (value instanceof Double)
      {
         return (Double) value;
      }
      else if (value instanceof SimpleString)
      {
         return Double.parseDouble(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Integer getIntProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Integer.valueOf(null);
      }
      else if (value instanceof Integer)
      {
         return (Integer) value;
      }
      else if (value instanceof Byte)
      {
         return ((Byte) value).intValue();
      }
      else if (value instanceof Short)
      {
         return ((Short) value).intValue();
      }
      else if (value instanceof SimpleString)
      {
         return Integer.parseInt(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Long getLongProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Long.valueOf(null);
      }
      else if (value instanceof Long)
      {
         return (Long) value;
      }
      else if (value instanceof Byte)
      {
         return ((Byte) value).longValue();
      }
      else if (value instanceof Short)
      {
         return ((Short) value).longValue();
      }
      else if (value instanceof Integer)
      {
         return ((Integer) value).longValue();
      }
      else if (value instanceof SimpleString)
      {
         return Long.parseLong(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid conversion");
      }
   }

   public Short getShortProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
      {
         return Short.valueOf(null);
      }
      else if (value instanceof Byte)
      {
         return ((Byte) value).shortValue();
      }
      else if (value instanceof Short)
      {
         return (Short) value;
      }
      else if (value instanceof SimpleString)
      {
         return Short.parseShort(((SimpleString) value).toString());
      }
      else
      {
         throw new HornetQPropertyConversionException("Invalid Conversion.");
      }
   }

   public Float getFloatProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);
      if (value == null)
         return Float.valueOf(null);
      if (value instanceof Float)
      {
         return ((Float) value);
      }
      if (value instanceof SimpleString)
      {
         return Float.parseFloat(((SimpleString) value).toString());
      }
      throw new HornetQPropertyConversionException("Invalid conversion: " + key);
   }

   public SimpleString getSimpleStringProperty(final SimpleString key) throws HornetQPropertyConversionException
   {
      Object value = doGetProperty(key);

      if (value == null)
      {
         return null;
      }

      if (value instanceof SimpleString)
      {
         return (SimpleString) value;
      }
      else if (value instanceof Boolean)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Character)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Byte)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Short)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Integer)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Long)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Float)
      {
         return new SimpleString(value.toString());
      }
      else if (value instanceof Double)
      {
         return new SimpleString(value.toString());
      }
      throw new HornetQPropertyConversionException("Invalid conversion");
   }

   public Object removeProperty(final SimpleString key)
   {
      return doRemoveProperty(key);
   }

   public boolean containsProperty(final SimpleString key)
   {
      if (size == 0)
      {
         return false;

      }
      else
      {
         return properties.containsKey(key);
      }
   }

   public Set<SimpleString> getPropertyNames()
   {
      if (size == 0)
      {
         return Collections.emptySet();
      }
      else
      {
         return properties.keySet();
      }
   }

   public synchronized void decode(final HornetQBuffer buffer)
   {
      byte b = buffer.readByte();

      if (b == DataConstants.NULL)
      {
         properties = null;
      }
      else
      {
         int numHeaders = buffer.readInt();

         properties = new HashMap<SimpleString, PropertyValue>(numHeaders);
         size = 0;

         for (int i = 0; i < numHeaders; i++)
         {
            int len = buffer.readInt();
            byte[] data = new byte[len];
            buffer.readBytes(data);
            SimpleString key = new SimpleString(data);

            byte type = buffer.readByte();

            PropertyValue val;

            switch (type)
            {
               case NULL:
               {
                  val = new NullValue();
                  doPutValue(key, val);
                  break;
               }
               case CHAR:
               {
                  val = new CharValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BOOLEAN:
               {
                  val = new BooleanValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BYTE:
               {
                  val = new ByteValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case BYTES:
               {
                  val = new BytesValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case SHORT:
               {
                  val = new ShortValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case INT:
               {
                  val = new IntValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case LONG:
               {
                  val = new LongValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case FLOAT:
               {
                  val = new FloatValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case DOUBLE:
               {
                  val = new DoubleValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               case STRING:
               {
                  val = new StringValue(buffer);
                  doPutValue(key, val);
                  break;
               }
               default:
               {
                  throw HornetQUtilBundle.BUNDLE.invalidType(type);
               }
            }
         }
      }
   }

   public synchronized void encode(final HornetQBuffer buffer)
   {
      if (properties == null)
      {
         buffer.writeByte(DataConstants.NULL);
      }
      else
      {
         buffer.writeByte(DataConstants.NOT_NULL);

         buffer.writeInt(properties.size());

         for (Map.Entry<SimpleString, PropertyValue> entry : properties.entrySet())
         {
            SimpleString s = entry.getKey();
            byte[] data = s.getData();
            buffer.writeInt(data.length);
            buffer.writeBytes(data);

            entry.getValue().write(buffer);
         }
      }
   }

   public int getEncodeSize()
   {
      if (properties == null)
      {
         return DataConstants.SIZE_BYTE;
      }
      else
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + size;
      }
   }

   public void clear()
   {
      if (properties != null)
      {
         properties.clear();
      }
   }

   @Override
   public String toString()
   {
      StringBuilder sb = new StringBuilder("TypedProperties[");


      if (properties != null)
      {

         Iterator<Entry<SimpleString, PropertyValue>> iter = properties.entrySet().iterator();

         while (iter.hasNext())
         {
            Entry<SimpleString, PropertyValue> iterItem = iter.next();
            sb.append(iterItem.getKey() + "=");

            // it seems weird but it's right!!
            // The first getValue is from the EntrySet
            // The second is to convert the PropertyValue into the actual value
            Object theValue = iterItem.getValue().getValue();


            if (theValue == null)
            {
               sb.append("NULL-value");
            }
            else if (theValue instanceof byte[])
            {
               sb.append("[" + ByteUtil.maxString(ByteUtil.bytesToHex((byte [])theValue, 2), 150) + ")");

               if (iterItem.getKey().toString().startsWith("_HQ_ROUTE_TO"))
               {
                  sb.append(",bytesAsLongs(");
                  try
                  {
                     ByteBuffer buff = ByteBuffer.wrap((byte[]) theValue);
                     while (buff.hasRemaining())
                     {
                        long bindingID = buff.getLong();
                        sb.append(bindingID);
                        if (buff.hasRemaining())
                        {
                           sb.append(",");
                        }
                     }
                  }
                  catch (Throwable e)
                  {
                     sb.append("error-converting-longs=" + e.getMessage());
                  }
                  sb.append("]");
               }
            }
            else
            {
               sb.append(theValue.toString());
            }


            if (iter.hasNext())
            {
               sb.append(",");
            }
         }
      }

      return sb.append("]").toString();
   }

   // Private ------------------------------------------------------------------------------------

   private void checkCreateProperties()
   {
      if (properties == null)
      {
         properties = new HashMap<SimpleString, PropertyValue>();
      }
   }

   private synchronized void doPutValue(final SimpleString key, final PropertyValue value)
   {
      if (key.startsWith(HQ_PROPNAME))
      {
         internalProperties = true;
      }

      PropertyValue oldValue = properties.put(key, value);
      if (oldValue != null)
      {
         size += value.encodeSize() - oldValue.encodeSize();
      }
      else
      {
         size += SimpleString.sizeofString(key) + value.encodeSize();
      }
   }

   private synchronized Object doRemoveProperty(final SimpleString key)
   {
      if (properties == null)
      {
         return null;
      }

      PropertyValue val = properties.remove(key);

      if (val == null)
      {
         return null;
      }
      else
      {
         size -= SimpleString.sizeofString(key) + val.encodeSize();

         return val.getValue();
      }
   }

   private synchronized Object doGetProperty(final Object key)
   {
      if (size == 0)
      {
         return null;
      }

      PropertyValue val = properties.get(key);

      if (val == null)
      {
         return null;
      }
      else
      {
         return val.getValue();
      }
   }

   // Inner classes ------------------------------------------------------------------------------

   private abstract static class PropertyValue
   {
      abstract Object getValue();

      abstract void write(HornetQBuffer buffer);

      abstract int encodeSize();

      @Override
      public String toString()
      {
         return "" + getValue();
      }
   }

   private static final class NullValue extends PropertyValue
   {
      public NullValue()
      {
      }

      @Override
      public Object getValue()
      {
         return null;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.NULL);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE;
      }

   }

   private static final class BooleanValue extends PropertyValue
   {
      final boolean val;

      public BooleanValue(final boolean val)
      {
         this.val = val;
      }

      public BooleanValue(final HornetQBuffer buffer)
      {
         val = buffer.readBoolean();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.BOOLEAN);
         buffer.writeBoolean(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_BOOLEAN;
      }

   }

   private static final class ByteValue extends PropertyValue
   {
      final byte val;

      public ByteValue(final byte val)
      {
         this.val = val;
      }

      public ByteValue(final HornetQBuffer buffer)
      {
         val = buffer.readByte();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.BYTE);
         buffer.writeByte(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_BYTE;
      }
   }

   private static final class BytesValue extends PropertyValue
   {
      final byte[] val;

      public BytesValue(final byte[] val)
      {
         this.val = val;
      }

      public BytesValue(final HornetQBuffer buffer)
      {
         int len = buffer.readInt();
         val = new byte[len];
         buffer.readBytes(val);
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.BYTES);
         buffer.writeInt(val.length);
         buffer.writeBytes(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + val.length;
      }

   }

   private static final class ShortValue extends PropertyValue
   {
      final short val;

      public ShortValue(final short val)
      {
         this.val = val;
      }

      public ShortValue(final HornetQBuffer buffer)
      {
         val = buffer.readShort();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.SHORT);
         buffer.writeShort(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_SHORT;
      }
   }

   private static final class IntValue extends PropertyValue
   {
      final int val;

      public IntValue(final int val)
      {
         this.val = val;
      }

      public IntValue(final HornetQBuffer buffer)
      {
         val = buffer.readInt();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.INT);
         buffer.writeInt(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_INT;
      }
   }

   private static final class LongValue extends PropertyValue
   {
      final long val;

      public LongValue(final long val)
      {
         this.val = val;
      }

      public LongValue(final HornetQBuffer buffer)
      {
         val = buffer.readLong();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.LONG);
         buffer.writeLong(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;
      }
   }

   private static final class FloatValue extends PropertyValue
   {
      final float val;

      public FloatValue(final float val)
      {
         this.val = val;
      }

      public FloatValue(final HornetQBuffer buffer)
      {
         val = Float.intBitsToFloat(buffer.readInt());
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.FLOAT);
         buffer.writeInt(Float.floatToIntBits(val));
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_FLOAT;
      }

   }

   private static final class DoubleValue extends PropertyValue
   {
      final double val;

      public DoubleValue(final double val)
      {
         this.val = val;
      }

      public DoubleValue(final HornetQBuffer buffer)
      {
         val = Double.longBitsToDouble(buffer.readLong());
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.DOUBLE);
         buffer.writeLong(Double.doubleToLongBits(val));
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_DOUBLE;
      }
   }

   private static final class CharValue extends PropertyValue
   {
      final char val;

      public CharValue(final char val)
      {
         this.val = val;
      }

      public CharValue(final HornetQBuffer buffer)
      {
         val = (char) buffer.readShort();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.CHAR);
         buffer.writeShort((short) val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_CHAR;
      }
   }

   private static final class StringValue extends PropertyValue
   {
      final SimpleString val;

      public StringValue(final SimpleString val)
      {
         this.val = val;
      }

      public StringValue(final HornetQBuffer buffer)
      {
         val = buffer.readSimpleString();
      }

      @Override
      public Object getValue()
      {
         return val;
      }

      @Override
      public void write(final HornetQBuffer buffer)
      {
         buffer.writeByte(DataConstants.STRING);
         buffer.writeSimpleString(val);
      }

      @Override
      public int encodeSize()
      {
         return DataConstants.SIZE_BYTE + SimpleString.sizeofString(val);
      }
   }

   public boolean isEmpty()
   {
      return properties.isEmpty();
   }

   public Map<String, Object> getMap()
   {
      Map<String, Object> m = new HashMap<String, Object>();
      for (Entry<SimpleString, PropertyValue> entry : properties.entrySet())
      {
         Object val = entry.getValue().getValue();
         if (val instanceof SimpleString)
         {
            m.put(entry.getKey().toString(), ((SimpleString) val).toString());
         }
         else
         {
            m.put(entry.getKey().toString(), val);
         }
      }
      return m;
   }

   /**
    * Helper for {@link MapMessage#setObjectProperty(String, Object)}
    *
    * @param key
    * @param value
    * @param properties
    */
   public static void setObjectProperty(final SimpleString key, final Object value, final TypedProperties properties)
   {
      if (value == null)
      {
         properties.putNullValue(key);
      }
      else if (value instanceof Boolean)
      {
         properties.putBooleanProperty(key, (Boolean) value);
      }
      else if (value instanceof Byte)
      {
         properties.putByteProperty(key, (Byte) value);
      }
      else if (value instanceof Character)
      {
         properties.putCharProperty(key, (Character) value);
      }
      else if (value instanceof Short)
      {
         properties.putShortProperty(key, (Short) value);
      }
      else if (value instanceof Integer)
      {
         properties.putIntProperty(key, (Integer) value);
      }
      else if (value instanceof Long)
      {
         properties.putLongProperty(key, (Long) value);
      }
      else if (value instanceof Float)
      {
         properties.putFloatProperty(key, (Float) value);
      }
      else if (value instanceof Double)
      {
         properties.putDoubleProperty(key, (Double) value);
      }
      else if (value instanceof String)
      {
         properties.putSimpleStringProperty(key, new SimpleString((String) value));
      }
      else if (value instanceof SimpleString)
      {
         properties.putSimpleStringProperty(key, (SimpleString) value);
      }
      else if (value instanceof byte[])
      {
         properties.putBytesProperty(key, (byte[]) value);
      }
      else
      {
         throw new HornetQPropertyConversionException(value.getClass() + " is not a valid property type");
      }
   }
}
