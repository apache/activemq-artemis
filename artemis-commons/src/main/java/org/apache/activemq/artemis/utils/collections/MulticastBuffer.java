/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.utils.collections;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import io.netty.util.internal.PlatformDependent;
import org.apache.activemq.artemis.utils.Env;
import sun.misc.Unsafe;

/**
 * Factory class to instantiate {@link Writer} and {@link Reader}s communicating through a shared multicast buffer.
 * A multicast buffer allowed a single {@link Writer} and can be read by zero or more {@link Reader}s concurrently.
 * The {@link Writer} is never back-pressured while writing, hence the {@link Reader}s could experience losses.
 * A {@link Reader} collects a count of the a {@link Reader#lost()} messages from the beginning of the {@link Writer}
 * transmissions.
 */
public final class MulticastBuffer {

   private static final Unsafe UNSAFE;

   static {
      Unsafe instance;
      try {
         Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
         field.setAccessible(true);
         instance = (sun.misc.Unsafe) field.get((Object) null);
      } catch (Throwable t) {
         try {
            Constructor<Unsafe> c = sun.misc.Unsafe.class.getDeclaredConstructor(new Class[0]);
            c.setAccessible(true);
            instance = c.newInstance(new Object[0]);
         } catch (Throwable t1) {
            throw new RuntimeException(t1);
         }
      }
      UNSAFE = instance;
   }

   private static long align(long value, int pow2Alignment) {
      return value + (long) (pow2Alignment - 1) & (long) (~(pow2Alignment - 1));
   }

   /**
    * Returns the required size in bytes of the {@link ByteBuffer} to be used as multicast buffer.
    *
    * @param messageSize the size in bytes of a message
    * @return the required size of the multicast buffer
    */
   public static int requiredSize(int messageSize) {
      final int requiredCapacity = MessageLayout.slotSize(messageSize);
      return requiredCapacity;
   }

   /**
    * Create a {@link Writer} that can write Inter-Process messages of {@code messageSize} bytes through the multicast
    * buffer mapped on the given {@code file}.
    * If {@code file} doesn't exist it will be created.
    *
    * @param file        the {@link File} where {@link FileChannel#map} the multicast buffer
    * @param messageSize the size in bytes of a message
    * @return a new {@link Writer} instance
    * @throws IOException
    */
   public static Writer writer(File file, final int messageSize) throws IOException {
      file.createNewFile();
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
         final long requiredSize = align(MessageLayout.slotSize(messageSize), Env.osPageSize());
         final MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, requiredSize);
         return new Writer(mappedByteBuffer, messageSize);
      }
   }

   /**
    * Create a {@link Reader} that can read Inter-Process messages of {@code messageSize} bytes through the multicast
    * buffer mapped on the given {@code file}.
    * If {@code file} doesn't exist a {@link FileNotFoundException} will be thrown.
    *
    * @param file        the {@link File} where {@link FileChannel#map} the multicast buffer
    * @param messageSize the size in bytes of a message
    * @return a new {@link Reader} instance
    * @throws FileNotFoundException If {@code file} doesn't exist
    * @throws IOException           If some other I/O error occurs
    */
   public static Reader reader(File file, final int messageSize) throws IOException {
      if (!file.exists()) {
         throw new FileNotFoundException("The requested file doesn't exists: " + file);
      }
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
         final long requiredSize = align(MessageLayout.slotSize(messageSize), Env.osPageSize());
         final MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, requiredSize);
         return new Reader(mappedByteBuffer, messageSize);
      }
   }

   /**
    * Create a {@link Writer} that can write Inter-Thread messages of {@code messageSize} bytes through the multicast
    * buffer mapped on the given direct {@link ByteBuffer}.
    * The direct {@link ByteBuffer} must have its base memory address {@link Long#BYTES}-aligned.
    *
    * @param buffer      the {@link ByteBuffer} where map the multicast buffer
    * @param messageSize the size in bytes of a message
    * @return a new {@link Writer} instance
    */
   public static Writer writer(ByteBuffer buffer, int messageSize) {
      return new Writer(buffer, messageSize);
   }

   /**
    * Create a {@link Reader} that can read Inter-Thread messages of {@code messageSize} bytes through the multicast
    * buffer mapped on the given direct {@code ByteBuffer}.
    * The direct {@link ByteBuffer} must have its base memory address {@link Long#BYTES}-aligned.
    *
    * @param buffer      the {@link ByteBuffer} where map the multicast buffer
    * @param messageSize the size in bytes of a message
    * @return a new {@link Reader} instance
    */
   public static Reader reader(final ByteBuffer buffer, final int messageSize) {
      return new Reader(buffer, messageSize);
   }

   private static final class MessageLayout {

      public static final int CLAIMED_VERSION_INDICATOR_SIZE = 8;
      public static final int COMMITTED_VERSION_INDICATOR_SIZE = 8;

      public static int slotSize(int messageSize) {
         return (int) align(messageSize + CLAIMED_VERSION_INDICATOR_SIZE + COMMITTED_VERSION_INDICATOR_SIZE, 8);
      }

      public static int committedVersionOffset(int slotSize) {
         return slotSize - 8;
      }

      public static int claimedVersionOffset(int slotSize) {
         return slotSize - 16;
      }

   }

   /**
    * Reads multicast messages written by a {@link Writer} via the underlying buffer.
    * If it can't keep up with the message stream, loss will be experienced, detected and counted.
    * <p>Any read operation on the {@link ByteBuffer} obtained with {@link #next()} must be performed without modifying
    * its content, {@link ByteBuffer#position()} and {@link ByteBuffer#limit()}: reccommended methods are the ones with absolute indexing
    * of the data to be read (eg {@link ByteBuffer#get(int)}, {@link ByteBuffer#getShort(int)}).</p>
    *
    *
    * <p>It is recommended practice to follow this usage pattern:
    *
    * <pre> {@code
    *    // ...
    *    if (reader.hasNext()) {
    *       final ByteBuffer next = reader.next();
    *       // ... performs read operations on next
    *       if (reader.validateMessage()) {
    *          // ... can use safely the data read from next
    *       }
    *    }
    *    // ...
    * }</pre>
    */
   public static final class Reader implements Iterator<ByteBuffer>, Iterable<ByteBuffer> {

      private final long bufferAddress;
      private final int messageSize;
      private final long claimedVersionAddress;
      private final long committedVersionAddress;
      private final ByteBuffer buffer;
      private long version;
      private long lost;

      private Reader(final ByteBuffer buffer, final int messageSize) {
         final int requiredCapacity = MessageLayout.slotSize(messageSize);
         if (buffer.capacity() < requiredCapacity) {
            throw new IllegalStateException("buffer is not big enough; required capacity is " + requiredCapacity + " bytes!");
         }
         this.bufferAddress = verifyBufferAlignment(buffer);
         //could be dangerous, always prefer to let writer and share share the same content but uses difference slices
         buffer.order(ByteOrder.nativeOrder());
         this.claimedVersionAddress = this.bufferAddress + MessageLayout.claimedVersionOffset(requiredCapacity);
         this.committedVersionAddress = this.bufferAddress + MessageLayout.committedVersionOffset(requiredCapacity);
         this.buffer = buffer;
         this.messageSize = messageSize;
         this.version = 0;
         this.lost = 0;
      }

      @Override
      public Iterator<ByteBuffer> iterator() {
         return this;
      }

      public int messageSize() {
         return messageSize;
      }

      public long version() {
         return version;
      }

      public ByteBuffer buffer() {
         return buffer;
      }

      public boolean validateMessage() {
         final long claimedVersionAddress = this.claimedVersionAddress;
         //The load fence (LoadLoad + LoadStore) has 2 meanings:
         //1) to prevent any load on buffer data to be performed after the validation check on the claimed version
         //2) to Read Acquire the claimed version via any load on buffer data
         UNSAFE.loadFence();
         final long claimedVersion = UNSAFE.getLong(null, claimedVersionAddress);
         final boolean isValid = claimedVersion == this.version;
         if (!isValid) {
            //the current version cannot be considered valid -> DO NOT CALL IT MULTIPLE TIMES!
            this.lost++;
         }
         return isValid;
      }

      public long lost() {
         return lost;
      }

      @Override
      public boolean hasNext() {
         final long committedVersionAddress = this.committedVersionAddress;
         final long committedVersion = UNSAFE.getLongVolatile(null, committedVersionAddress);
         //LoadLoad + LoadStore: Read-Acquire the message content + the claimed version via committed version
         final long currentVersion = this.version;
         //the version of the data is old?
         if (committedVersion <= currentVersion) {
            //invalidate current copy to avoid any further reading to the message copy buffer!
            return false;
         }
         final long expectedCommittedVersion = currentVersion + 1;
         //loss detection: the perceived committed version could be ahead our expected version
         final long lostFromLastHasNext = (committedVersion - expectedCommittedVersion);
         this.lost += lostFromLastHasNext;
         this.version = committedVersion;
         //we're optimistic: the data is not overwritten yet :)
         return true;
      }

      @Override
      public ByteBuffer next() {
         //return the last valid copy of a message
         return buffer;
      }
   }

   private static long verifyBufferAlignment(ByteBuffer buffer) {
      final long address = PlatformDependent.directBufferAddress(buffer);
      if (address % Long.BYTES != 0) {
         throw new IllegalStateException("the buffer is not correctly aligned: addressOffset=" + address + " is not divisible by " + Long.BYTES);
      }
      return address;
   }

   /**
    * Writes multicast messages that can be read by zero or more {@link Reader}s via the underlying buffer.
    *
    * <p>It is recommended practice to follow this usage pattern:
    *
    * <pre> {@code
    *    // ...
    *    final long version = writer.claim();
    *    try {
    *       //perform operations on writer.buffer()
    *    } finally {
    *       writer.commit(version);
    *    }
    *    // ...
    * }</pre>
    */
   public static final class Writer {

      private final long bufferAddress;
      private final int messageSize;
      private final long claimedVersionAddress;
      private final long committedVersionAddress;
      private final ByteBuffer buffer;

      private Writer(final ByteBuffer buffer, final int messageSize) {
         final int requiredCapacity = MessageLayout.slotSize(messageSize);
         if (buffer.capacity() < requiredCapacity) {
            throw new IllegalStateException("buffer is not big enough; required capacity is " + requiredCapacity + " bytes!");
         }
         this.bufferAddress = verifyBufferAlignment(buffer);
         //force native order for performance reasons
         buffer.order(ByteOrder.nativeOrder());
         this.claimedVersionAddress = this.bufferAddress + MessageLayout.claimedVersionOffset(requiredCapacity);
         this.committedVersionAddress = this.bufferAddress + MessageLayout.committedVersionOffset(requiredCapacity);
         this.buffer = buffer;
         this.messageSize = messageSize;
      }

      public int messageSize() {
         return this.messageSize;
      }

      public ByteBuffer buffer() {
         return buffer;
      }

      public long claim() {
         final long claimedVersionAddress = this.claimedVersionAddress;
         final long claimedVersion = UNSAFE.getLong(claimedVersionAddress);
         final long nextClaimedVersion = claimedVersion + 1;
         //StoreStore + LoadStore to avoid the previous commit operation to float after this claim
         //publish claimed version === the intent to start writing the message content
         UNSAFE.putOrderedLong(null, claimedVersionAddress, nextClaimedVersion);
         //the store fence force the write of the claimed version to be performed before any write operations on the buffer
         UNSAFE.storeFence();
         //StoreStore + LoadStore
         //any message content written after the explicit fence will act as a Write-Release that will publish the last written claimed version too.
         //A reader can use this leak to recognize if a Read-Acquired content is no longer valid by checking that the leaked value of the claim is not changed
         //from the last committed value read.
         return nextClaimedVersion;
      }

      public void commit(long version) {
         //StoreStore + LoadStore
         //the Write-Release of the committed version allows a reader that perform a Read-Acquire on it to read safely the message content
         //until a new claim will change it.
         UNSAFE.putOrderedLong(null, this.committedVersionAddress, version);
      }

   }

}
