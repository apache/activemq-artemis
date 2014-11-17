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
package org.apache.activemq.tests.unit.util;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.api.core.HornetQBuffers;
import org.apache.activemq.tests.util.UnitTestCase;
import org.apache.activemq.utils.HornetQBufferInputStream;
import org.junit.Test;

/**
 * A HornetQInputStreamTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class HornetQBufferInputStreamTest extends UnitTestCase
{

   @Test
   public void testReadBytes() throws Exception
   {
      byte[] bytes = new byte[10 * 1024];
      for (int i = 0; i < bytes.length; i++)
      {
         bytes[i] = getSamplebyte(i);
      }

      HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bytes);
      HornetQBufferInputStream is = new HornetQBufferInputStream(buffer);

      // First read byte per byte
      for (int i = 0; i < 1024; i++)
      {
         assertEquals(getSamplebyte(i), is.read());
      }

      // Second, read in chunks
      for (int i = 1; i < 10; i++)
      {
         bytes = new byte[1024];
         is.read(bytes);
         for (int j = 0; j < bytes.length; j++)
         {
            assertEquals(getSamplebyte(i * 1024 + j), bytes[j]);
         }

      }

      assertEquals(-1, is.read());


      bytes = new byte[1024];

      int sizeRead = is.read(bytes);

      assertEquals(-1, sizeRead);
      is.close();
   }
}
