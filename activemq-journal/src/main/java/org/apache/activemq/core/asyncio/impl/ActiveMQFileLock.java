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
package org.apache.activemq.core.asyncio.impl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.activemq.core.libaio.Native;

/**
 * A ActiveMQFileLock
 * @author clebertsuconic
 */
public class ActiveMQFileLock extends FileLock
{

   private final int handle;

   protected ActiveMQFileLock(final int handle)
   {
      super((FileChannel)null, 0, 0, false);
      this.handle = handle;
   }

   @Override
   public boolean isValid()
   {
      return true;
   }

   @Override
   public void release() throws IOException
   {
      Native.closeFile(handle);
   }
}
