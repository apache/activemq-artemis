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
package org.apache.activemq.artemis.utils.collections;

import java.util.function.ToLongFunction;

public interface LinkedList<E> {

   void addHead(E e);

   void addTail(E e);

   E poll();

   LinkedListIterator<E> iterator();

   void clear();

   int size();

   void clearID();

   /** The ID Supplier function needs to return positive IDs (greater or equal to 0)
    *  If you spply a negative ID, it will be considered a null value, and
    *  the value will just be ignored. */
   void setIDSupplier(ToLongFunction<E> supplier);

   E removeWithID(long id);
}
