/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 *
 */
public class StubTransportListener implements TransportListener {

   private final Queue<Object> commands = new ConcurrentLinkedQueue<>();
   private final Queue<Object> exceptions = new ConcurrentLinkedQueue<>();

   public Queue<Object> getCommands() {
      return commands;
   }

   public Queue<Object> getExceptions() {
      return exceptions;
   }

   @Override
   public void onCommand(Object command) {
      commands.add(command);
   }

   @Override
   public void onException(IOException error) {
      exceptions.add(error);
   }

   @Override
   public void transportInterupted() {
   }

   @Override
   public void transportResumed() {
   }

}
