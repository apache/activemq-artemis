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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class ActiveMQThreadFactory implements ThreadFactory {

   private String groupName;

   private final AtomicInteger threadCount = new AtomicInteger(0);

   private final int threadPriority;

   private final boolean daemon;

   private final ClassLoader tccl;

   private final AccessControlContext acc;

   private final String prefix;

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param groupName the name of the thread group to assign threads to by default
    * @param daemon    whether the created threads should be daemon threads
    * @param tccl      the context class loader of newly created threads
    */
   public ActiveMQThreadFactory(final String groupName, final boolean daemon, final ClassLoader tccl) {
      this(groupName, "Thread-", daemon, tccl);
   }

   /**
    * Construct a new instance.  The access control context of the calling thread will be the one used to create
    * new threads if a security manager is installed.
    *
    * @param groupName the name of the thread group to assign threads to by default
    * @param daemon    whether the created threads should be daemon threads
    * @param tccl      the context class loader of newly created threads
    */
   public ActiveMQThreadFactory(final String groupName, String prefix, final boolean daemon, final ClassLoader tccl) {
      this.groupName = groupName;

      this.prefix = prefix;

      this.threadPriority = Thread.NORM_PRIORITY;

      this.tccl = tccl;

      this.daemon = daemon;

      this.acc = AccessController.getContext();
   }

   @Override
   public Thread newThread(final Runnable command) {
      // create a thread in a privileged block if running with Security Manager
      if (acc != null) {
         return AccessController.doPrivileged(new ThreadCreateAction(command), acc);
      } else {
         return createThread(command);
      }
   }

   private final class ThreadCreateAction implements PrivilegedAction<Thread> {

      private final Runnable target;

      private ThreadCreateAction(final Runnable target) {
         this.target = target;
      }

      @Override
      public Thread run() {
         return createThread(target);
      }
   }

   private Thread createThread(final Runnable command) {
      final Thread t = new Thread(command, prefix + threadCount.getAndIncrement() + " (" + groupName + ")");
      t.setDaemon(daemon);
      t.setPriority(threadPriority);
      t.setContextClassLoader(tccl);
      return t;
   }

   public static ActiveMQThreadFactory defaultThreadFactory() {
      String callerClassName = Thread.currentThread().getStackTrace()[2].getClassName();
      return new ActiveMQThreadFactory(callerClassName, false, null);
   }

}
