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

package org.apache.activemq.artemis.utils.actors;

/**
 * This abstract class will encapsulate
 * ThreadLocals to determine when a class is a handler.
 * This is because some functionality has to be avoided if inHandler().
 *
 */
public abstract class HandlerBase {

   //marker instance used to recognize if a thread is performing a packet handling
   private static final Object DUMMY = Boolean.TRUE;

   // this cannot be static as the Actor will be used within another executor. For that reason
   // each instance will have its own ThreadLocal.
   // ... a thread that has its thread-local map populated with DUMMY while performing a handler
   private final ThreadLocal<Object> inHandler = new ThreadLocal<>();

   protected void enter() {
      assert inHandler.get() == null : "should be null";
      inHandler.set(DUMMY);
   }

   public boolean inHandler() {
      final Object dummy = inHandler.get();
      return dummy != null;
   }

   protected void leave() {
      assert inHandler.get() != null : "marker not set";
      inHandler.set(null);
   }

}
