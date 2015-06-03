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
package org.proton.plug.test.minimalserver;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class SimpleServerThreadFactory implements ThreadFactory
{
   private final ThreadGroup group;

   private final AtomicInteger threadCount = new AtomicInteger(0);

   private final int threadPriority;

   private final boolean daemon;

   private final ClassLoader tccl;

   public SimpleServerThreadFactory(final String groupName, final boolean daemon, final ClassLoader tccl)
   {
      group = new ThreadGroup(groupName + "-" + System.identityHashCode(this));

      this.threadPriority = Thread.NORM_PRIORITY;

      this.tccl = tccl;

      this.daemon = daemon;
   }

   public Thread newThread(final Runnable command)
   {
      final Thread t;
      // attach the thread to a group only if there is no security manager:
      // when sandboxed, the code does not have the RuntimePermission modifyThreadGroup
      if (System.getSecurityManager() == null)
      {
         t = new Thread(group, command, "Thread-" + threadCount.getAndIncrement() + " (" + group.getName() + ")");
      }
      else
      {
         t = new Thread(command, "Thread-" + threadCount.getAndIncrement());
      }

      AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            t.setDaemon(daemon);
            t.setPriority(threadPriority);
            return null;
         }
      });

      try
      {
         AccessController.doPrivileged(new PrivilegedAction<Object>()
         {
            public Object run()
            {
               t.setContextClassLoader(tccl);
               return null;
            }
         });
      }
      catch (java.security.AccessControlException e)
      {
         e.printStackTrace();
      }

      return t;
   }
}
