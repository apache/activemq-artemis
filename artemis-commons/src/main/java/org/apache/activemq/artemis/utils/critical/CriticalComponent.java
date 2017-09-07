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
package org.apache.activemq.artemis.utils.critical;


/**
 * A Critical component enters and leaves a critical state.
 * You update a long every time you enter a critical path
 * you update a different long with a System.nanoTime every time you leave that path.
 *
 * If the enterCritical > leaveCritical at any point, then you need to measure the timeout.
 * if the system stops responding, then you have something irresponsive at the system.
 */
public interface CriticalComponent {

   /**
    * please save the time you entered here.
    * Use volatile variables.
    * No locks through anywhere.
    */
   default void enterCritical(int path) {
      // I'm providing a default as some components may chose to calculate it themselves
   }

   /**
    * please save the time you entered here
    * Use volatile variables.
    * No locks through anywhere.
    */
   default void leaveCritical(int path) {
      // I'm providing a default as some components may chose to calculate it themselves
   }

   /**
    * Is this Component expired at a given timeout.. on any of its paths.
    * @param timeout
    * @return -1 if it's ok, or the number of the path it failed
    */
   boolean isExpired(long timeout);
}