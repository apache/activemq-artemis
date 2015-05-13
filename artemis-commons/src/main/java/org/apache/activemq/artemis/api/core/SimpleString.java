/**
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
package org.apache.activemq.artemis.api.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.utils.DataConstants;

/**
 * A simple String class that can store all characters, and stores as simple {@code byte[]}, this
 * minimises expensive copying between String objects.
 * <p>
 * This object is used heavily throughout ActiveMQ Artemis for performance reasons.
 *
 */
public final class SimpleString implements CharSequence, Serializable, Comparable<SimpleString>
{
   private static final long serialVersionUID = 4204223851422244307L;

   // Attributes
   // ------------------------------------------------------------------------
   private final byte[] data;

   private transient int hash;

   // Cache the string
   private transient String str;

   // Static
   // ----------------------------------------------------------------------

   /**
    * Returns a SimpleString constructed from the {@code string} parameter.
    * <p>
    * If {@code string} is {@code null}, the return value will be {@code null} too.
    *
    * @param string String used to instantiate a SimpleString.
    * @return A new SimpleString
    */
   public static SimpleString toSimpleString(final String string)
   {
      if (string == null)
      {
         return null;
      }
      return new SimpleString(string);
   }

   // Constructors
   // ----------------------------------------------------------------------

   /**
    * creates a SimpleString from a conventional String
    *
    * @param string the string to transform
    */
   public SimpleString(final String string)
   {
      int len = string.length();

      data = new byte[len << 1];

      int j = 0;

      for (int i = 0; i < len; i++)
      {
         char c = string.charAt(i);

         byte low = (byte) (c & 0xFF); // low byte

         data[j++] = low;

         byte high = (byte) (c >> 8 & 0xFF); // high byte

         data[j++] = high;
      }

      str = string;
   }

   /**
    * creates a SimpleString from a byte array
    *
    * @param data the byte array to use
    */
   public SimpleString(final byte[] data)
   {
      this.data = data;
   }

   // CharSequence implementation
   // ---------------------------------------------------------------------------

   public int length()
   {
      return data.length >> 1;
   }

   public char charAt(int pos)
   {
      if (pos < 0 || pos >= data.length >> 1)
      {
         throw new IndexOutOfBoundsException();
      }
      pos <<= 1;

      return (char) ((data[pos] & 0xFF) | (data[pos + 1] << 8) & 0xFF00);
   }

   public CharSequence subSequence(final int start, final int end)
   {
      int len = data.length >> 1;

      if (end < start || start < 0 || end > len)
      {
         throw new IndexOutOfBoundsException();
      }
      else
      {
         int newlen = end - start << 1;
         byte[] bytes = new byte[newlen];

         System.arraycopy(data, start << 1, bytes, 0, newlen);

         return new SimpleString(bytes);
      }
   }

   // Comparable implementation -------------------------------------

   public int compareTo(final SimpleString o)
   {
      return toString().compareTo(o.toString());
   }

   // Public
   // ---------------------------------------------------------------------------

   /**
    * returns the underlying byte array of this SimpleString
    *
    * @return the byte array
    */
   public byte[] getData()
   {
      return data;
   }

   /**
    * returns true if the SimpleString parameter starts with the same data as this one. false if not.
    *
    * @param other the SimpleString to look for
    * @return true if this SimpleString starts with the same data
    */
   public boolean startsWith(final SimpleString other)
   {
      byte[] otherdata = other.data;

      if (otherdata.length > data.length)
      {
         return false;
      }

      for (int i = 0; i < otherdata.length; i++)
      {
         if (data[i] != otherdata[i])
         {
            return false;
         }
      }

      return true;
   }

   @Override
   public String toString()
   {
      if (str == null)
      {
         int len = data.length >> 1;

         char[] chars = new char[len];

         int j = 0;

         for (int i = 0; i < len; i++)
         {
            int low = data[j++] & 0xFF;

            int high = data[j++] << 8 & 0xFF00;

            chars[i] = (char) (low | high);
         }

         str = new String(chars);
      }

      return str;
   }

   @Override
   public boolean equals(final Object other)
   {
      if (this == other)
      {
         return true;
      }

      if (other instanceof SimpleString)
      {
         SimpleString s = (SimpleString) other;

         if (data.length != s.data.length)
         {
            return false;
         }

         for (int i = 0; i < data.length; i++)
         {
            if (data[i] != s.data[i])
            {
               return false;
            }
         }

         return true;
      }
      else
      {
         return false;
      }
   }

   @Override
   public int hashCode()
   {
      if (hash == 0)
      {
         int tmphash = 0;
         for (byte element : data)
         {
            tmphash = (tmphash << 5) - tmphash + element; // (hash << 5) - hash is same as hash * 31
         }
         hash = tmphash;
      }

      return hash;
   }

   /**
    * Splits this SimpleString into an array of SimpleString using the char param as the delimiter.
    * i.e. "a.b" would return "a" and "b" if . was the delimiter
    *
    * @param delim The delimiter to split this SimpleString on.
    * @return An array of SimpleStrings
    */
   public SimpleString[] split(final char delim)
   {
      List<SimpleString> all = null;

      byte low = (byte) (delim & 0xFF); // low byte
      byte high = (byte) (delim >> 8 & 0xFF); // high byte

      int lasPos = 0;
      for (int i = 0; i < data.length; i += 2)
      {
         if (data[i] == low && data[i + 1] == high)
         {
            byte[] bytes = new byte[i - lasPos];
            System.arraycopy(data, lasPos, bytes, 0, bytes.length);
            lasPos = i + 2;

            // We will create the ArrayList lazily
            if (all == null)
            {
               // There will be at least 2 strings on this case (which is the actual common usecase)
               // For that reason I'm allocating the ArrayList with 2 already
               // I have thought about using LinkedList here but I think this will be good enough already
               // Note by Clebert
               all = new ArrayList<SimpleString>(2);
            }
            all.add(new SimpleString(bytes));
         }
      }

      if (all == null)
      {
         return new SimpleString[]{this};
      }
      else
      {
         // Adding the last one
         byte[] bytes = new byte[data.length - lasPos];
         System.arraycopy(data, lasPos, bytes, 0, bytes.length);
         all.add(new SimpleString(bytes));

         // Converting it to arrays
         SimpleString[] parts = new SimpleString[all.size()];
         return all.toArray(parts);
      }
   }

   /**
    * checks to see if this SimpleString contains the char parameter passed in
    *
    * @param c the char to check for
    * @return true if the char is found, false otherwise.
    */
   public boolean contains(final char c)
   {
      final byte low = (byte) (c & 0xFF); // low byte
      final byte high = (byte) (c >> 8 & 0xFF); // high byte

      for (int i = 0; i < data.length; i += 2)
      {
         if (data[i] == low && data[i + 1] == high)
         {
            return true;
         }
      }
      return false;
   }

   /**
    * Concatenates a SimpleString and a String
    *
    * @param toAdd the String to concatenate with.
    * @return the concatenated SimpleString
    */
   public SimpleString concat(final String toAdd)
   {
      return concat(new SimpleString(toAdd));
   }

   /**
    * Concatenates 2 SimpleString's
    *
    * @param toAdd the SimpleString to concatenate with.
    * @return the concatenated SimpleString
    */
   public SimpleString concat(final SimpleString toAdd)
   {
      byte[] bytes = new byte[data.length + toAdd.getData().length];
      System.arraycopy(data, 0, bytes, 0, data.length);
      System.arraycopy(toAdd.getData(), 0, bytes, data.length, toAdd.getData().length);
      return new SimpleString(bytes);
   }

   /**
    * Concatenates a SimpleString and a char
    *
    * @param c the char to concate with.
    * @return the concatenated SimpleString
    */
   public SimpleString concat(final char c)
   {
      byte[] bytes = new byte[data.length + 2];
      System.arraycopy(data, 0, bytes, 0, data.length);
      bytes[data.length] = (byte) (c & 0xFF);
      bytes[data.length + 1] = (byte) (c >> 8 & 0xFF);
      return new SimpleString(bytes);
   }

   /**
    * returns the size of this SimpleString
    *
    * @return the size
    */
   public int sizeof()
   {
      return DataConstants.SIZE_INT + data.length;
   }

   /**
    * returns the size of a SimpleString
    *
    * @param str the SimpleString to check
    * @return the size
    */
   public static int sizeofString(final SimpleString str)
   {
      return str.sizeof();
   }

   /**
    * returns the size of a SimpleString which could be null
    *
    * @param str the SimpleString to check
    * @return the size
    */
   public static int sizeofNullableString(final SimpleString str)
   {
      if (str == null)
      {
         return 1;
      }
      else
      {
         return 1 + str.sizeof();
      }
   }

   /**
    * This method performs a similar function to {@link String#getChars(int, int, char[], int)}.
    * This is mainly used by the Parsers on Filters
    *
    * @param srcBegin The srcBegin
    * @param srcEnd The srcEnd
    * @param dst The destination array
    * @param dstPos The destination position
    */
   public void getChars(final int srcBegin, final int srcEnd, final char[] dst, final int dstPos)
   {
      if (srcBegin < 0)
      {
         throw new StringIndexOutOfBoundsException(srcBegin);
      }
      if (srcEnd > length())
      {
         throw new StringIndexOutOfBoundsException(srcEnd);
      }
      if (srcBegin > srcEnd)
      {
         throw new StringIndexOutOfBoundsException(srcEnd - srcBegin);
      }

      int j = srcBegin * 2;
      int d = dstPos;

      for (int i = srcBegin; i < srcEnd; i++)
      {
         int low = data[j++] & 0xFF;

         int high = data[j++] << 8 & 0xFF00;

         dst[d++] = (char) (low | high);
      }
   }

}
