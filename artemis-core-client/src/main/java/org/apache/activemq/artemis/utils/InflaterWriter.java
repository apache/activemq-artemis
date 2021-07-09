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
package org.apache.activemq.artemis.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * InflaterWriter
 * <p>
 * This class takes an OutputStream. Compressed bytes
 * can directly be written into this class. The class will
 * decompress the bytes and write them to the output stream.
 * <p>
 * Not for concurrent use.
 */
public class InflaterWriter extends OutputStream {

   private final Inflater inflater = new Inflater();

   private final OutputStream output;

   private final byte[] writeBuffer = new byte[1024];

   private int writePointer = 0;

   private final byte[] outputBuffer = new byte[writeBuffer.length * 2];

   public InflaterWriter(final OutputStream output) {
      this.output = output;
   }

   private void checkClosed() {
      if (writePointer < 0) {
         throw new IllegalStateException("The writer is already closed");
      }
   }

   /*
    * Write a compressed byte.
    */
   @Override
   public void write(final int b) throws IOException {
      checkClosed();
      writeBuffer[writePointer] = (byte) (b & 0xFF);
      writePointer++;

      if (writePointer == writeBuffer.length) {
         writePointer = 0;
         doWrite(writeBuffer.length);
      }
   }

   @Override
   public void close() throws IOException {
      if (writePointer < 0) {
         // ignore if already closed
         return;
      }
      try {
         if (writePointer > 0) {
            doWrite(writePointer);
         }
         output.close();
      } finally {
         // close it
         writePointer = -1;
         // release native Inflater resources
         inflater.end();
      }
   }

   private void doWrite(int length) throws IOException {
      try {
         inflater.setInput(writeBuffer, 0, length);
         int n = inflater.inflate(outputBuffer);

         while (n > 0) {
            output.write(outputBuffer, 0, n);
            n = inflater.inflate(outputBuffer);
         }
      } catch (DataFormatException e) {
         IOException ie = new IOException("Error decompressing data");
         ie.initCause(e);
         throw ie;
      }
   }

}
